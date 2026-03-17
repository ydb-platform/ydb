********
dictpath
********


About
#####

Object-oriented dictionary paths

Key features
************

* Traverse resources like paths
* Access resources on demand with separate accessor layer

Usage
#####

.. code-block:: python

   from dictpath import DictPath
   
   d = {
       "parts": {
           "part1": {
               "name": "Part One",
           },
           "part2": {
               "name": "Part Two",
           },
       },
   }
   
   dp = DictPath(d)
   
   # Concatenate paths with /
   parts = dp / "parts"
   
   # Stat path keys
   "part2" in parts
   
   # Open path dict
   with parts.open() as parts_dict:
       print(parts_dict)

