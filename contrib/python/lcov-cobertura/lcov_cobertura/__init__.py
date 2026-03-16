"""
Coverage conversion tool to make lcov output compatible with tools that
expect Cobertura/coverage.py format.
"""
from lcov_cobertura.lcov_cobertura import LcovCobertura


__all__ = ["LcovCobertura"]
