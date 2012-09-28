drop table if exists sih_i_part;
drop table if exists sih_src;
drop table if exists sih_src2;

set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=300;
set mapred.min.split.size=300;
set mapred.min.split.size.per.node=300;
set mapred.min.split.size.per.rack=300;
set hive.exec.mode.local.auto=true;
set hive.merge.smallfiles.avgsize=1;

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.20)

-- create file inputs
create table sih_i_part (key int, value string) partitioned by (p string);
insert overwrite table sih_i_part partition (p='1') select key, value from src;
insert overwrite table sih_i_part partition (p='2') select key+10000, value from src;
insert overwrite table sih_i_part partition (p='3') select key+20000, value from src;
create table sih_src as select key, value from sih_i_part order by key, value;
create table sih_src2 as select key, value from sih_src order by key, value;

set hive.exec.post.hooks = org.apache.hadoop.hive.ql.hooks.VerifyIsLocalModeHook ;
set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=does.notexist.com:666;
set hive.exec.mode.local.auto.input.files.max=1;
set hive.sample.seednumber=7;

-- sample split, running locally limited by num tasks
select count(1) from sih_src tablesample(1 percent);

set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=does.notexist.com:666;
set hive.sample.seednumber=7;

-- sample two tables
select count(1) from sih_src tablesample(1 percent)a join sih_src2 tablesample(1 percent)b on a.key = b.key;

set hive.exec.mode.local.auto.inputbytes.max=1000;
set hive.exec.mode.local.auto.input.files.max=4;
set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=does.notexist.com:666;
set hive.sample.seednumber=7;

-- sample split, running locally limited by max bytes
select count(1) from sih_src tablesample(1 percent);

drop table sih_i_part;
drop table sih_src;
drop table sih_src2;
