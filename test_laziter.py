from laziter import laziter


def test_pass():
    assert True


def sq(a):
    return a ** 2


negsq = (
        laziter([1, 2, [3, [4, range(10000), 69]]])
        .flatten()
        .drop(8888)
        .parmap(sq)
        .parmap(lambda a: -a, mp_backend="threading", n_cpus=3)
        .parmap(lambda a: a * 2, mp_backend="pathos", chunksize=16)
        .filter(lambda a: a % 2 == 0)
        .map(lambda a: -a)
    )


def test_parmap():
    next(iter(negsq))

def test_take():
    assert list(negsq.take(5)) == [
        157850912,
        157886450,
        157921992,
        157957538,
        157993088,
    ]

def test_reduce():
    assert negsq.take(20).reduce(int.__add__) == 3163775020

def test_slice():
    assert list(negsq[0:10]) == [
        157850912,
        157886450,
        157921992,
        157957538,
        157993088,
        158028642,
        158064200,
        158099762,
        158135328,
        158170898,
    ]
