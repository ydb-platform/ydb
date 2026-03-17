 # jsonsubschema #

 [![Travis build status](https://travis-ci.com/IBM/jsonsubschema.svg?branch=master)](https://travis-ci.com/IBM/jsonsubschema) [![Codecov code coverage](https://codecov.io/gh/IBM/jsonsubschema/branch/master/graph/badge.svg)](https://codecov.io/gh/IBM/jsonsubschema)

**jsonsubschema** checks if one JSON schema is a subschema (subtype) of another.

For any two JSON schemas s1 and s2, s1 <: s2 (reads s1 is subschema/subtype of s2) 
if every JSON document instance that validates against s1 also validates against s2.

jsonsubschema is very useful in analysing schema evolution and ensuring that newer schema versions are backward compatible.
jsonsubschema also enables static type checking on different components of a system that uses JSON schema to describe data 
interfaces among the system's different components.

The details of JSON subschema are covered in our [**ISSTA 2021** paper](https://dl.acm.org/doi/10.1145/3460319.3464796),
which received a [Distinguished Artifact Award](https://conf.researchr.org/details/issta-2021/issta-2021-technical-papers/2/Finding-Data-Compatibility-Bugs-with-JSON-Subschema-Checking): 

```
@InProceedings{issta21JSONsubschema,
  author    = {Habib, Andrew and Shinnar, Avraham and Hirzel, Martin and Pradel, Michael},
  title     = {Finding Data Compatibility Bugs with JSON Subschema Checking},
  booktitle = {The ACM SIGSOFT International Symposium on Software Testing and Analysis (ISSTA)},
  year      = {2021},
  pages     = {620--632},
  url       = {https://doi.org/10.1145/3460319.3464796},
}
```


## I) Obtaining the tool ##

### Requirements ###

* python 3.8.*
* Other python dependencies will be installed during the below setup process

You can either install subschema from the source code from github or the pypy package.

### A) Install from github source code ###
Execute the following:
```
git clone https://github.com/IBM/jsonsubschema.git 
cd jsonsubschema
python setup.py install
cd ..
```

### B) Install from pypy ###
Execute the following:
```
pip install jsonsubschema
```

## II) Running  subschema ##

JSON subschema provides two usage interfaces:

### A) CLI interface ###
1. Create two JSON schema examples by executing the following:
```
echo '{"type": ["null", "string"]}' > s1.json
echo '{"type": ["string", "null"], "not": {"enum": [""]}}' > s2.json
```

2. Invoke the CLI by executing:
```
python -m jsonsubschema.cli s2.json s1.json
```

### B) python API ###
```
from jsonsubschema import isSubschema

def main():
	s1 = {'type': "integer"}
	s2 = {'type': ["integer", "string"]}
	
	print(f'LHS <: RHS {isSubschema(s1, s2)}')

if __name__ == "__main__":
	main()
```



## License

jsonsubschema is distributed under the terms of the Apache 2.0
License, see [LICENSE.txt](LICENSE.txt).

## Contributions

json-subschema is still at an early phase of development and we
welcome contributions. Contributors are expected to submit a
'Developer's Certificate of Origin', which can be found in
[DCO1.1.txt](DCO1.1.txt).
