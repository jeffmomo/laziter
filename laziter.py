import multiprocessing
import multiprocessing.dummy
import itertools
import signal
from collections.abc import Iterable, Iterator
from typing import TypeVar, Union, Iterable as IterableType, Iterator as IteratorType, Any, List, Optional, Callable


T = TypeVar('T')
U = TypeVar('U')


def _init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def nats() -> IteratorType[int]:
    num = 1
    while True:
        yield num
        num += 1


def map_fn(iterable: IterableType[T], fn: Callable[[T], U]) -> IterableType[U]:
    for item in iterable:
        yield fn(item)


def _base_parmap(pool, iterable: IterableType[T], fn: Callable[[T], U], chunksize: int) -> IterableType[U]:
    try:
        yield from pool.imap(fn, iterable, chunksize)
        pool.close()
    except (KeyboardInterrupt, Exception) as e:
        pool.terminate()
        raise e
    finally:
        pool.join()

def parmap_multiprocessing_fn(iterable: IterableType[T], fn: Callable[[T], U], n_cpus: int, chunksize: int) -> IterableType[U]:
    pool = multiprocessing.Pool(n_cpus, _init_worker)
    yield from _base_parmap(pool, iterable, fn, chunksize)


def parmap_threading_fn(iterable: IterableType[T], fn: Callable[[T], U], n_cpus: int, chunksize: int) -> IterableType[U]:
    pool = multiprocessing.dummy.Pool(n_cpus)
    yield from _base_parmap(pool, iterable, fn, chunksize)


def parmap_pathos_fn(iterable: IterableType[T], fn: Callable[[T], U], n_cpus: int, chunksize: int) -> IterableType[U]:
    from multiprocess.pool import Pool

    pool = Pool(n_cpus, _init_worker)
    yield from _base_parmap(pool, iterable, fn, chunksize)


def flatten_fn(iterable: IterableType[Union[IterableType, Any]]) -> IterableType[Any]:
    for item in iterable:
        if isinstance(item, Iterable) and not isinstance(item, str):
            yield from flatten_fn(item)
        else:
            yield item


def take_fn(iterable: IterableType[T], n: int) -> IterableType[T]:
    for _ in range(n):
        yield next(iterable)


def drop_fn(iterable: IterableType[T], n: int) -> IterableType[T]:
    for _ in range(n):
        next(iterable)
    yield from iterable


def filter_fn(iterable: IterableType[T], fn: Callable[[T], bool]) -> IterableType[T]:
    for item in iterable:
        if fn(item):
            yield item


mp_backends = {
    'multiprocessing': parmap_multiprocessing_fn,
    'pathos': parmap_pathos_fn,
    'threading': parmap_threading_fn,
}


class laziter:
    def __init__(self, iterable_or_list: Union[IterableType[Any], List[Any]]):
        self._base_iter = iterable_or_list
        self._history: List[FuncObj] = []

    def _get_base_iterator(self):
        if isinstance(self._base_iter, Iterator):
            return self._base_iter

        return iter(self._base_iter)

    def _with_computation(self, fn: Callable, *args: Any) -> 'laziter':
        new_laziter = laziter(self._get_base_iterator())
        new_laziter._history = [*self._history, FuncObj(fn, *args)]
        return new_laziter

    def map(self, fn: Callable[[T], U]) -> 'laziter':
        """
        Performs a standard map

        :param fn: The function to map over the iterable
        """
        return self._with_computation(map_fn, fn)

    def filter(self, fn: Callable[[T], bool]) -> 'laziter':
        """
        Given a predicate, filters the collection

        :param fn: A function that takes the item and returns a boolean
        """
        return self._with_computation(filter_fn, fn)

    def take(self, n: int) -> 'laziter':
        """
        Takes n items from the collection

        :param n: The number of items
        :return: Returns a new instance of 'laziter' with n items
        """
        return self._with_computation(take_fn, n)

    def drop(self, n: int) -> 'laziter':
        """
        Ignores n items in the collection

        :param n: The number of items
        :return: A new instance of 'laziter' with n items skipped
        """
        return self._with_computation(drop_fn, n)

    def flatten(self) -> 'laziter':
        """
        Fully flattens the iterator.
        Gives special treatment for strings, which are not flattened.

        :return: An instance of 'laziter' with any nested iterables flattened out
        """
        return self._with_computation(flatten_fn)

    def parmap(self, fn: Callable[[T], U], n_cpus: int = -1, chunksize: int = 1, mp_backend: str = 'multiprocessing') -> 'laziter':
        """
        Performs a map with one of 3 mappers:
            ``multiprocessing``: The default Python multiprocessing pool

            ``pathos``: Uses Pathos multiprocesses, which allows lambda functions

            ``threading``: Uses Python threads, which are subject to the GIL but works well for IO-bound tasks

        :param fn: Mapper function
        :param n_cpus: Number of processes to use. Defaults to `cpu_count()`
        :return: An instance of 'laziter' with the parallel map applied
        """
        assert mp_backend in mp_backends, f'mp_backend "{mp_backend}" not in {list(mp_backends.values())}'

        n_cpus = multiprocessing.cpu_count() if n_cpus < 0 else n_cpus

        return self._with_computation(mp_backends[mp_backend], fn, n_cpus, chunksize)

    def __iter__(self):
        """
        Gets the iterator for this instance and applies all specified computations

        :return: The iterator for the computed collection
        """
        iterable = self._get_base_iterator()
        for funcobj in self._history:
            iterable = funcobj.function(iterable, *funcobj.args)

        return iterable

    def __getitem__(self, item):
        if isinstance(item, slice):
            return itertools.islice(self, item.start, item.stop, item.step)
        else:
            iterable = iter(self)
            for _ in range(item):
                next(iterable)
            return next(iterable)

    def reduce(self, fn: Callable[[T, U], U], initial: Optional[U] = None) -> U:
        """
        Performs a left-to-right fold on the collection.

        :param fn: The reduction function fn(item: T, accumulator: U) -> U
        :param initial: The initial accumulator. If `None`, uses the first value in the computed collection
        :return: The result of reduction
        """
        iterable = iter(self)
        accumulator = initial or next(iterable)
        for item in iterable:
            accumulator = fn(item, accumulator)

        return accumulator

    def compute(self) -> 'laziter':
        """
        Forces a computation and caches it.
        Any subsequent computation will continue from the current state.

        :return: An instance of 'laziter' with the computed value cached
        """
        self._base_iter = list(self)
        self._history = []
        return self

class FuncObj:
    def __init__(self, fn: Callable, *args: Any):
        self.function = fn
        self.args = args

def sq(a):
    return a ** 2

if __name__ == '__main__':
    negsq = laziter([1, 2, [3, [4, range(1000000), 69]]])\
        .flatten().drop(8888)\
        .parmap(sq)\
        .filter(lambda a: a % 2 == 0)\
        .map(lambda a: -a)\

    for i in negsq.take(5):
        print(i)
    print(negsq.take(20).reduce(int.__add__))


    print(sum(negsq.take(50)))
    print(list(negsq))
    print(1231231, list(negsq[0:10]))
