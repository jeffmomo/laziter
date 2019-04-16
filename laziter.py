import multiprocessing
import itertools
import time

from functools import partial
from collections.abc import Iterable, Iterator
from typing import TypeVar, Union, Iterable as IterableType, Iterator as IteratorType, Any, List, Callable


T = TypeVar('T')
U = TypeVar('U')


def nats() -> IteratorType[int]:
    num = 1
    while True:
        yield num
        num += 1


def map_fn(iterable: IterableType[T], fn: Callable[[T], U]) -> IteratorType[U]:
    for item in iterable:
        yield fn(item)


def parmap_python_fn(iterable: IterableType[T], fn: Callable[[T], U], n_cpus: int) -> IterableType[U]:
    pool = multiprocessing.Pool(n_cpus)
    yield from pool.imap(fn, iterable)


def parmap_pathos_fn(iterable: IterableType[T], fn: Callable[[T], U], n_cpus: int) -> IteratorType[U]:
    from multiprocess.pool import Pool

    pool = Pool(n_cpus)
    yield from pool.imap(fn, iterable)


def flatten_fn(iterable: IterableType[Union[IterableType, Any]]) -> IteratorType[Any]:
    for item in iterable:
        if isinstance(item, Iterable):
            yield from flatten_fn(item)
        else:
            yield item


def take_fn(iterable: IterableType[T], n: int) -> IteratorType[T]:
    for _ in range(n):
        yield next(iterable)


def drop_fn(iterable: IterableType[T], n: int):
    for _ in range(n):
        next(iterable)
    yield from iterable


def filter_fn(iterable: IterableType[T], fn: Callable[[T], bool]) -> IteratorType[T]:
    for item in iterable:
        if fn(item):
            yield item


mp_backends = {
    'python': parmap_python_fn,
    'pathos': parmap_pathos_fn,
}


class laziter:
    def __init__(self, iterable_or_list: Union[IterableType, List], mp_backend='python'):
        self._base_iter = iterable_or_list
        self._mp_backend = mp_backend
        self._history = []

        assert mp_backend in mp_backends

    def _get_base_iterator(self):
        if isinstance(self._base_iter, Iterator):
            return self._base_iter

        return iter(self._base_iter)

    def _with_computation(self, fn, *args) -> 'laziter':
        new_laziter = laziter(self._get_base_iterator())
        new_laziter._history = [*self._history, FuncObj(fn, *args)]
        return new_laziter

    def map(self, fn: Callable[[T], U]) -> 'laziter':
        return self._with_computation(map_fn, fn)

    def filter(self, fn: Callable[[T], bool]) -> 'laziter':
        return self._with_computation(filter_fn, fn)

    def take(self, n: int) -> 'laziter':
        return self._with_computation(take_fn, n)

    def drop(self, n: int) -> 'laziter':
        return self._with_computation(drop_fn, n)

    def flatten(self) -> 'laziter':
        return self._with_computation(flatten_fn)

    def parmap(self, fn: Callable[[T], U], n_cpus=multiprocessing.cpu_count()) -> 'laziter':
        return self._with_computation(mp_backends[self._mp_backend], fn, n_cpus)

    def __iter__(self):
        iterable = self._get_base_iterator()
        for funcobj in self._history:
            iterable = funcobj.function(iterable, *funcobj.args)

        return iterable
        # return iter(iterable) if isinstance(iterable, Iterable) else iterable

    def __getitem__(self, item):
        if isinstance(item, slice):
            return itertools.islice(self, item.start, item.stop, item.step)
        else:
            iterable = iter(self)
            for _ in range(item):
                next(iterable)
            return next(iterable)

    def reduce(self, fn: Callable[[T, U], U], initial: Union[U, None] = None) -> U:
        iterable = iter(self)
        accumulator = initial or next(iterable)
        for item in iterable:
            accumulator = fn(item, accumulator)

        return accumulator

    def compute(self) -> 'laziter':
        self._base_iter = list(self)
        self._history = []
        return self

class FuncObj:
    def __init__(self, fn, *args):
        self.function = fn
        self.args = args

def sq(a):
    # time.sleep(2)
    return a ** 2

if __name__ == '__main__':
    negsq = laziter([1, 2, [3, [4, range(1000000), 69]]], mp_backend='python')\
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