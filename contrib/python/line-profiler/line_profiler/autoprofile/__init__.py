"""
Auto-Profiling
==============

In cases where you want to decorate every function in a script, module, or
package decorating every function with ``@profile`` can be tedious. To make
this easier "auto-profiling" was introduced to ``line_profiler`` in Version
4.1.0.

The "auto-profile" feature allows users to specify a script or module.
This is done by passing the name of script or module to kernprof.

To demonstrate auto-profiling, we first need to generate a Python script to
profile. Write the following code to a file called ``demo_primes2.py``.

.. code:: python

    def is_prime(n):
        '''
        Check if the number "n" is prime, with n > 1.

        Returns a boolean, True if n is prime.
        '''
        max_val = n ** 0.5
        stop = int(max_val + 1)
        for i in range(2, stop):
            if n % i == 0:
                return False
        return True


    def find_primes(size):
        primes = []
        for n in range(size):
            flag = is_prime(n)
            if flag:
                primes.append(n)
        return primes


    def main():
        print('start calculating')
        primes = find_primes(100000)
        print(f'done calculating. Found {len(primes)} primes.')


    if __name__ == '__main__':
        main()

Note that this is the nearly the same "primes" example from the "Basic Usage"
section in :py:mod:`line_profiler`, but none of the functions are decorated
with ``@profile``.

To run this script with auto-profiling we invoke kernprof with ``-p`` or
``--prof-mod`` and pass the names of the script. When running with ``-p`` we
must also run with ``-l`` to enable line profiling. In this example we include
``-v`` as well to display the output after we run it.

.. code:: bash

    python -m kernprof -lv -p demo_primes2.py demo_primes2.py


The output will look like this:

.. code::


    start calculating
    done calculating. Found 9594 primes.
    Wrote profile results to demo_primes2.py.lprof
    Timer unit: 1e-06 s

    Total time: 0.677348 s
    File: demo_primes2.py
    Function: is_prime at line 4

    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
         4                                           def is_prime(n):
         5                                               '''
         6                                               Check if the number "n" is prime, with n > 1.
         7
         8                                               Returns a boolean, True if n is prime.
         9                                               '''
        10    100000      19921.6      0.2      2.9      math.floor(1.3)
        11    100000      17743.6      0.2      2.6      max_val = n ** 0.5
        12    100000      23962.7      0.2      3.5      stop = int(max_val + 1)
        13   2745693     262005.7      0.1     38.7      for i in range(2, stop):
        14   2655287     342216.1      0.1     50.5          if n % i == 0:
        15     90406      10401.4      0.1      1.5              return False
        16      9594       1097.2      0.1      0.2      return True


    Total time: 1.56657 s
    File: demo_primes2.py
    Function: find_primes at line 19

    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
        19                                           def find_primes(size):
        20         1          0.3      0.3      0.0      primes = []
        21    100000      11689.5      0.1      0.7      for n in range(size):
        22    100000    1541848.0     15.4     98.4          flag = is_prime(n)
        23     90406      10260.0      0.1      0.7          if flag:
        24      9594       2775.9      0.3      0.2              primes.append(n)
        25         1          0.2      0.2      0.0      return primes


    Total time: 1.61013 s
    File: demo_primes2.py
    Function: main at line 28

    Line #      Hits         Time  Per Hit   % Time  Line Contents
    ==============================================================
        28                                           def main():
        29         1         17.6     17.6      0.0      print('start calculating')
        30         1    1610081.3    2e+06    100.0      primes = find_primes(100000)
        31         1         26.6     26.6      0.0      print(f'done calculating. Found {len(primes)} primes.')

"""
