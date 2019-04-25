# laziter
Lazy iterators for Python 3, in the spirit of LINQ or Java Stream. Has easy multithreading built in

# Installation
Simply `pip install laziter`. It's lightweight and has no dependencies (if you choose not to use the `pathos` multiprocess implementation)

# Usage
```py
from laziter import laziter
import time

def sleeper(x):
    time.sleep(2)
    return x

lz = laziter([2, 3, [range(1000), 90], 9])

lz2 = (lz
    .flatten()
    .filter(lambda a: a % 2)
    .skip(998)
    .take(4)
    .map(lambda x: x * x)
    .parmap(sleeper)
)

print(sum(lz2))
print(list(lz2[2:4]))
```

# Documentation

[https://laziter.readthedocs.io](https://laziter.readthedocs.io)


