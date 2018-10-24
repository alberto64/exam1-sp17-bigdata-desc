create table schools(region string, district string, city string, SCID int, scname string, sclevel string, CBID int) row format delimited fields terminated by ',';
load data local inpath '/home/alberto_dejesus/escuelasPR.csv' into table schools;
create table students(region string, district string, SCID int, scname string, sclevel string, sex string, SID int) row format delimited fields terminated by ',';
load data local inpath '/home/alberto_dejesus/studentsPR.csv' into table students;

--Q1
select schools.region, city, count(*) from students, schools where students.SCID = schools.SCID group by schools.region, city;
--Q2
select city, sclevel, count(*) from schools group by city, sclevel;
--Q3
select count(*) from students, schools where students.SCID = schools.SCID and students.sex = 'F' and students.sclevel = 'SPR' and schools.city = 'Ponce';
--Q4
select schools.region, schools.district, schools.city, count(*) from students, schools where students.SCID = schools.SCID and students.sex = 'M' group by schools.region, schools.district, schools.city;

