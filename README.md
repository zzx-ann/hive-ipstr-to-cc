项目介绍  从mysql的IP数据库把ip转换成对应的区域

使用方式
1.把jar包上传到hdfs 
[hive@ybnode110 udf]$ hdfs dfs -put iptocc.jar /home/hive/udf/     

2.注册成永久函数 函数信息存储在元数据库的default库 查看注册函数的使用方式
hive> create function default.iptoarea as 'xbl.IpToCcByMysql' using jar 'hdfs://192.168.50.110:8020/home/hive/udf/iptocc.jar';
converting to local hdfs://192.168.50.110:8020/home/hive/udf/iptocc.jar
Added [/tmp/f73f4724-a45e-4e94-a591-8e13579ced38_resources/iptocc.jar] to class path
Added resources: [hdfs://192.168.50.110:8020/home/hive/udf/iptocc.jar]
OK
Time taken: 0.149 seconds
hive> desc function default.iptoarea ;
OK
returns 'country_province_city', where ipstr is whatever you give it (string)
Time taken: 0.054 seconds, Fetched: 1 row

3.查看hive 用户注册的函数
mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| hivemeta           |
| oozie              |
| performance_schema |
+--------------------+
7 rows in set (0.00 sec)
mysql> use hivemeta;
Database changed
mysql> select * from FUNC_RU;
+---------+---------------+-----------------------------------------------------+-------------+
| FUNC_ID | RESOURCE_TYPE | RESOURCE_URI                                        | INTEGER_IDX |
+---------+---------------+-----------------------------------------------------+-------------+
|       1 |             1 | hdfs://ybnode110/home/hive/udf/sofa.jar             |           0 |
|       6 |             1 | hdfs://192.168.50.110:8020/home/hive/udf/iptocc.jar |           0 |
+---------+---------------+-----------------------------------------------------+-------------+
2 rows in set (0.00 sec)

4. 使用例子
hive> select 20180501,a.p,a.c,count(*) from (select split(default.iptoarea(collect_list(ip)[0]),'_')[1] as p,split(default.iptoarea(collect_list(ip)[0]),'_')[2] as c from newlog.xblappstart where ymd=20180501  group by concat(udid,deviceid) ) a  group by a.p,a.c;
converting to local hdfs://192.168.50.110:8020/home/hive/udf/iptocc.jar
Added [/tmp/f3505295-5863-46f3-b019-f937ae5a50b4_resources/iptocc.jar] to class path
Added resources: [hdfs://192.168.50.110:8020/home/hive/udf/iptocc.jar]
Query ID = hive_20180503204921_e3b29d2f-454d-4051-9d54-8b30de19a590
Total jobs = 1
Launching Job 1 out of 1


Status: Running (Executing on YARN cluster with App id application_1522469205044_7402)

--------------------------------------------------------------------------------
        VERTICES      STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED
--------------------------------------------------------------------------------
Map 1 ..........   SUCCEEDED      7          7        0        0       0       0
Reducer 2 ......   SUCCEEDED      1          1        0        0       0       0
Reducer 3 ......   SUCCEEDED      1          1        0        0       0       0
--------------------------------------------------------------------------------
VERTICES: 03/03  [==========================>>] 100%  ELAPSED TIME: 295.13 s   
--------------------------------------------------------------------------------
OK
20180501        ipformat        ipformat        2
20180501        ipisnull        ipisnull        2
20180501        unknown unknown 90495
20180501        上海    上海市  27512
20180501        云南    unknown 5448
20180501        云南    临沧市  1801
20180501        云南    丽江市  378
20180501        云南    保山市  910
...


问题1 
要确保整个Hadoop集群的机器要有连接对应数据库的权限
问题2
如果不把mysql驱动打包的jar包里面，要把mysql驱动文件传到hdfs里面，存放在linux文件系统不起作用。
