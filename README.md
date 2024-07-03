# Please add your team members' names here. 

## Team members' names 

1. Student Name: Emin Koroglu

   Student UT EID: enk438

2. Student Name: John Vu

   Student UT EID: jv32765

3. Student Name: Kamil Yildirim

   Student UT EID: ky5637

4. Student Name: Fatima Camci

   Student UT EID: fdc362

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 84744

# Project Report

```
The images are in the docs/ folder!
```

## Task 1
With definition of valid lines as:
* md5sums are 32 in length & hexadecimal only

```
EE06BD8A621CAC3B608ACFDF0585A76A 357
11DC93DD66D8A9A3DD9223122CF99EFD 353
6C1132EF70BC0A7DB02174592F9A64A1 349
23DB792D3F7EBA03004E470B684F2738 347
A10A65AFD9F401BF3BDB79C84D3549E7 346
738A62EEE9EC371689751A864C5EF811 345
7DA8DF1E4414F81EBD3A0140073B2630 342
021F88711374FB07E02F03A06637C5C8 338
7D93E7FC4A7E4615A34B8286D92FF57F 338
A5DD2A1A64A56572806EE7E1F96F62D5 337
```

## Task 2
With definition of valid lines as:
* driver_id's md5sum is 32 in length & hexadecimal only
* trip seconds is > 0
* total fare amount is >= 0
```
FD2AE1C5F9F5FBE73A6D6D3D33270571 4095.0
A7C9E60EEE31E4ADC387392D37CD06B8 1260.0
D8E90D724DBD98495C1F41D125ED029A 630.0
E9DA1D289A7E321CC179C51C0C526A73 231.3
74071A673307CA7459BCF75FBD024E09 210.0
95A921A9908727D4DC03B5D25A4B0F62 210.0
42AB6BEE456B102C1CF8D9D8E71E845A 191.55
28EAF0C54680C6998F0F2196F2DA2E21 180.0
FA587EC2731AAB9F2952622E89088D4B 180.0
E79402C516CEF1A6BB6F526A142597D4 144.54545454545456
```

## Task 3
With definition of valid lines as:
* pickup time is a valid date
* surcharge is >= 0
* travel distance is > 0
```
17 0.13833025606548552
18 0.13555823467037492
19 0.11014553482467819
```

## Conclusion/Remarks
The times for the job history is a little misleading. The jobs were created at the same time, but the job that was created first hogged all the resources leading the other jobs to stagnate, but still elapse time.

## Describe here your project
This is a pyspark project that calculates the:
* main_task1.py - the taxi cab with the most unique drivers
* main_task2.py - the driver with the most money per minute
* main_task3.py - the hour of which is the most profitable

# How to run  
```python
spark-submit main_task1.py 
```
```python
spark-submit main_task2.py 
```
```python
spark-submit main_task3.py 
```

## Running locally for testing
```python
spark-submit --master local[*] main_task1.py
```
