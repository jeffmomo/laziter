from setuptools import setup
from pathlib import Path

setup(
    name='laziter',
    url='https://github.com/jeffmomo/laziter',
    author='Jeff Mo',
    description='A lazy iterator wrapper with multiprocessing powers!',
    long_description=(Path(__file__).parent/'README.md').open('r').read(),
    version='0.0.6',
    py_modules=['laziter'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
