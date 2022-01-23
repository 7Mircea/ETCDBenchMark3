# etapa 4

# How to run

In order to run this example you have to execute :  
```
cd
etcd
```
First in order to go to home folder for current user and the second for starting a etcd store.
The program uses at least : 121_555_200B = 121.5MB(aprox) of RAM.
In order to compile and run the program.  
```
gradle clean  
gradle build  
gradle run  
```

# Testing environment used by author

Linux 20.04, Intel i7, 16GB RAM, java 17.

# Testing

Before you run the program you have to write this in a terminal:  
```
etcd  
```
To check if the etcd store is working correctly:  
```
etcdctl put cheie valoare  
etcdctl get cheie  
```
If you want to delete:  
```
etcdctl del cheie  
```

## Configuration

You could configure the scale factor(how many lines to be inserted) by changing scaleFactorArr, the number of times the insert operation is repetead by changing repeatNr.  
You should definitly change the path of the file with values to be inserted.

# Result

## Inseration

Scalation factor : 10000  
Inseration time:8039.0s  
Inseration time:6608.0s  
Inseration time:6445.0s  
Inseration time:6407.0s  
Inseration time:6320.0s  
Mean : 6763.8. Std dev: 644.4077591090909  

Scalation factor : 100000  
Inseration time:63622.0s  
Inseration time:66698.0s  
Inseration time:68952.0s  
Inseration time:68697.0s  
Inseration time:67464.0s  
Mean : 67086.6. Std dev: 1916.7680715203912  

Scalation factor : 200000  
Inseration time:133955.0s  
Inseration time:129730.0s  
Inseration time:131649.0s  
Inseration time:131543.0s  
Inseration time:130715.0s  
Mean : 131518.4. Std dev: 1399.8933673676722  
  
Scalation factor : 400000  
Inseration time:262260.0s  
Inseration time:268585.0s  
Inseration time:267583.0s  
Inseration time:265803.0s  
Inseration time:266425.0s  
Mean : 266131.2. Std dev: 2159.1239334507873  

Scalation factor : 405184  
Inseration time:267925.0s  
Inseration time:267258.0s  
Inseration time:268205.0s  
Inseration time:270758.0s  
Inseration time:271942.0s  
Mean : 269217.6. Std dev: 1807.284880698115  

# Updating

update
Update time:13.0 ms
Update time:5.0 ms
Update time:7.0 ms
Update time:5.0 ms
Update time:5.0 ms
Update time:5.0 ms
Update time:4.0 ms
Update time:4.0 ms
Update time:5.0 ms
Update time:5.0 ms
Mean : 5.8 ms. Std dev: 2.5219040425836985

## Deleting

delete
Delete time:6.0 ms.
Delete time:2.0 ms.
Delete time:2.0 ms.
Delete time:3.0 ms.
Delete time:2.0 ms.
Delete time:2.0 ms.
Delete time:2.0 ms.
Delete time:2.0 ms.
Delete time:2.0 ms.
Delete time:2.0 ms.
Mean : 2.5 ms. Std dev: 1.204159457879229

## Select 1
(was run also 10 times but because the select's result was shown I couldn't see every value.)
Select time:79723.0 ms.
Select time:78587.0 ms.
Mean : 80694.6 ms. Std dev: 2732.1526018873838

## Select 2

time:78465.0 ms.
time:76925.0 ms.
time:76221.0 ms.
time:76645.0 ms.
time:74911.0 ms.
time:74703.0 ms.
time:74598.0 ms.
time:78234.0 ms.
time:77887.0 ms.
time:74903.0 ms.
Mean : 76349.2 ms. Std dev: 1441.0405129627688

## Select 3

time:75861.0 ms.
time:72426.0 ms.
time:72170.0 ms.
time:72479.0 ms.
time:73051.0 ms.
time:73032.0 ms.
time:72271.0 ms.
time:72423.0 ms.
time:72343.0 ms.
time:72288.0 ms.
Mean : 72834.4 ms. Std dev: 1049.1659735237317

## Select 4

<pre>time:77092.0 ms.
time:72706.0 ms.
time:72551.0 ms.
time:72790.0 ms.
time:72713.0 ms.
time:72698.0 ms.
time:72757.0 ms.
time:73078.0 ms.
time:73079.0 ms.
time:72718.0 ms.
Mean : 73218.2 ms. Std dev: 1300.9596304267093</pre>
