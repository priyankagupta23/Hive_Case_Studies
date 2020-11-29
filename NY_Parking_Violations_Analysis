create table parking(`summons_number` int, `plate_id` string, `regis_state` string, `plate_type` string, `issue_date` string, `violation_code` int, `violation_body_type` string,
`vehicle_make` string, `issue_agency` string, `street_code1` int, `street_code2` int, `street_code3` int, `vehicle_exp_date` int, `violation_location` int, `violation_precinct` int, `issuer_precinct` int,
`issuer_code` int, `issuer_command` string, `issuer_squad` string, `violation_time` string, `first_time_observed` string, `violation_country` string, `violation_front_or_opposite` string, `house_number` string,
`street_name` string, `intersecting_street` string, `date_first_observed` int, `law_section` int, `sub_division` string, `violation_legal_code` string, `days_parking_in_effect` string, `from_hrs_in_effect` string,
`to_hrs_in_effect` string, `vehicle_color` string, `unregistered_vehicle` int, `vehicle_year` int, `meter_number` string, `feet_from_curb` int, `vehicle_post_code` string, `viol_descr` string, `no_stop_stand_viol` string,
`hydration_violation` string, `double_parking_violation` string) row format delimited fields terminated by ',' stored as textfile tblproperties("skip.header.line.count"="1");

load data local inpath '/home/priyanka/parking' overwrite into table parking;

select count(*) as count from parking;

select count(distinct regis_state) as no_of_states from parking;

select count(*) from parking where street_code1 is null or street_code2 is null or street_code3 is null;

select violation_code, count(violation_code) as frequency from parking group by violation_code order by frequency desc limit 5;

select vehicle_body_type, count(vehicle_body_type) as frequency from parking group by vehicle_body_type order by frequency desc limit 5;

select vehicle_make, count(vehicle_make) as frequency from parking group by vehicle_make order by frequency desc limit 5;

select violation_precinct, count(violation_precinct) as frequency from parking group by violation_precinct order by frequency desc limit 5;

select issuer_precinct, count(issuer_precinct) as frequency from parking group by issuer_precinct order by frequency desc limit 5;

select a.issuer_precinct as issuer_precinct, b.violation_code as violation_code, a.rank as rank, b.count as count from (select * from (select issuer_precinct, rank() 
over (order by count(*) desc) as rank from parking group by issuer_precinct)t where rank<=3)a left join 
(select issuer_precinct, violation_code, count(*) as count from parking group by issuer_precinct, violation_code)b on a.issuer_precinct = b.issuer_precinct; 

select hour(from_unixtime(unix_timestamp(concat(issue_date, ' ', violation_time, 'M'), 'MM/dd/yyyy hhmma'))) as time_of_day, count(*) as count from parking group by
hour(from_unixtime(unix_timestamp(concat(issue_date, ' ', violation_time, 'M'), 'MM/dd/yyyy hhmma'))) order by time_of_day;

select * from
(select time_period, violation_code, count, rank() over (partition by time_period order by count desc) as rank from
 (select round(hour(from_unixtime(unix_timestamp(concat(issue_date, ' ', violation_time, 'M'), 'MM/dd/yyyy hhmma')))/4) as time_period, violation_code, count(*) as count from parking group by
round(hour(from_unixtime(unix_timestamp(concat(issue_date, ' ', violation_time, 'M'), 'MM/dd/yyyy hhmma')))/4), violation_code) a) b where rank <=3;

create table parking_orc(`summons_number` int, `plate_id` string, `regis_state` string, `plate_type` string, `issue_date` string, `violation_code` int, `violation_body_type` string,
`vehicle_make` string, `issue_agency` string, `street_code1` int, `street_code2` int, `street_code3` int, `vehicle_exp_date` int, `violation_location` int, `violation_precinct` int, `issuer_precinct` int,
`issuer_code` int, `issuer_command` string, `issuer_squad` string, `violation_time` string, `first_time_observed` string, `violation_country` string, `violation_front_or_opposite` string, `house_number` string,
`street_name` string, `intersecting_street` string, `date_first_observed` int, `law_section` int, `sub_division` string, `violation_legal_code` string, `days_parking_in_effect` string, `from_hrs_in_effect` string,
`to_hrs_in_effect` string, `vehicle_color` string, `unregistered_vehicle` int, `vehicle_year` int, `meter_number` string, `feet_from_curb` int, `vehicle_post_code` string, `viol_descr` string, `no_stop_stand_viol` string,
`hydration_violation` string, `double_parking_violation` string) partitioned by (month string) row format delimited fields terminated by ',' stored as orc tblproperties("orc.compress"="snappy");

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table parking_orc partition(month) select *, substr(issue_date, 1, 2) as month from parking where (issue_date like '%2017');

select distinct (year(from_unixtime(unix_timestamp(issue_date, 'MM/dd/yyyy')))) from parking_orc;

select * from
(select season, violation_code, count, rank() over (partition by season order by count desc) as rank from
(select case
when month in ('03','04','05') then 'spring'
when month in ('06','07','08') then 'summer'
when month in ('09','10','11') then 'fall'
when month in ('12','01','02') then 'winter'
end as season, 
violation_code, count(*) as count from parking_orc group by case
when month in ('03','04','05') then 'spring'
when month in ('06','07','08') then 'summer'
when month in ('09','10','11') then 'fall'
when month in ('12','01','02') then 'winter'
end, violation_code) a) b where rank<=3;
