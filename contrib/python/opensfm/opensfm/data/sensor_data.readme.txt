--------------------------------------------
Sensor Database for Cameras
____________________________________________

This is a database of camera sensors (mainly CCD sensor size now). The first version of the database (~3600 digital cameras) is contributed by Gregor Brdnik, the creator of http://www.digicamdb.com/. 

Any further contributions to the database are encouraged and welcome. 

--------------------------------------------
License
--------------------------------------------
This database is licensed under the same license as OpenSfM which is Simplified BSD license. 

--------------------------------------------
Contents
--------------------------------------------
The database contains two json files, 'sensor_data.json' and 'sensor_data_detailed.json'. 

sensor_data.json : A slim version of the database - a dictionary that contains only the camera model and the sensor size in mm. Each item in the dictionary is in the form of 'MAKE MODEL: SENSOR_SIZE in mm'. In general, 'MAKE' and 'MODEL' are available in the EXIF of an image. For example, given MAKE='Canon', MODEL='EOS 1000D', one will be able to query the sensor size from the dictionary {"Canon EOS 1000D": 22.2}. 

sensor_data_detailed.json : A detailed version of database that contains more complete information about the sensors.








