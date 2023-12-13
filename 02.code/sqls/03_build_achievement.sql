-- CREATE PERFORMANCE MONTHS TABLE
CREATE TEMP TABLE ach_dates as 
SELECT DISTINCT ACH_DATE from supp_pcn_data ORDER BY ACH_DATE desc;

DROP TABLE if EXISTS supp_months_achievement;
CREATE TABLE supp_months_achievement as 
SELECT ACH_DATE,
CASE
when ACH_DATE<=20230331 then '22/23'
else '23/24' end as FY,
-(ROW_NUMBER() OVER(ORDER BY ACH_DATE desc)-1) as M_PAST,
case 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 4 then 1 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 5 then 2 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 6 then 3 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 7 then 4 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 8 then 5 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 9 then 6 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 10 then 7 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 11 then 8 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 12 then 9 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 1 then 10 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 2 then 11 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 3 then 12 
end as M_FY,
case 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 4 then 11 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 5 then 10 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 6 then 9
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 7 then 8 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 8 then 7 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 9 then 6 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 10 then 5 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 11 then 4 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 12 then 3 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 1 then 2 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 2 then 1 
when cast(SUBSTR(ACH_DATE,5,2) as integer) = 3 then 0 
end as M_TO_YE
from ach_dates;


-- create national averages for patching null submissions in payout projections
CREATE temp TABLE nat_avg AS
select a.ACH_DATE, a.IND_CD, sum(a.NUM)/sum(a.DEN) as NAT_ACH, b.M_PAST
from supp_pcn_data as a left join supp_months_achievement as b
on a.ACH_DATE = b.ACH_DATE 
where a.SUBMIT_STATUS="TRUE" and b.M_PAST>=-3 group by a.ACH_DATE, a.IND_CD;

DROP TABLE if EXISTS supp_pcn_achievement_nat_avg;
create TABLE supp_pcn_achievement_nat_avg AS
SELECT a.IND_CD, b.NAT_ACH as Nm0, c.NAT_ACH as Nm1, d.NAT_ACH as Nm2, e.NAT_ACH as Nm3
from 
(select distinct IND_CD from nat_avg) as a 
left JOIN (select IND_CD, NAT_ACH from nat_avg where M_PAST =0) as b on a.IND_CD = b.IND_CD 
left JOIN (select IND_CD, NAT_ACH from nat_avg where M_PAST =-1) as c on a.IND_CD = c.IND_CD 
left JOIN (select IND_CD, NAT_ACH from nat_avg where M_PAST =-2) as d on a.IND_CD = d.IND_CD 
left JOIN (select IND_CD, NAT_ACH from nat_avg where M_PAST =-3) as e on a.IND_CD = e.IND_CD;

-- CREATE ACTUAL ACHIEVEMENT 
CREATE TEMP TABLE actual_achievement as 
SELECT *, 
case 
    when DEN is NULL then NULL
    when DEN = 0 then 0
    else min(NUM/DEN,1) 
end as P_IND_ACH
from supp_pcn_data; 


-- CREATE PROJECTED YE ACHIEVEMENT

-- create last performance table
CREATE TEMP TABLE last_achievement as
SELECT  a.PCN_CD, a.IND_CD, a.ACH_DATE,  b.M_PAST, a.P_IND_ACH 
from actual_achievement as a 
left join ( select ACH_DATE, M_PAST  from supp_months_achievement where M_PAST in (0,-1,-2,-3)) as b on a.ACH_DATE = b.ACH_DATE 
where b.M_PAST is not NULL;

CREATE TEMP TABLE head as 
select DISTINCT PCN_CD, IND_CD from last_achievement;
create TEMP TABLE m0 as 
select PCN_CD, IND_CD, P_IND_ACH from last_achievement where M_PAST = 0;
create TEMP TABLE m1 as 
select PCN_CD, IND_CD, P_IND_ACH from last_achievement where M_PAST = -1;
create TEMP TABLE m2 as 
select PCN_CD, IND_CD, P_IND_ACH from last_achievement where M_PAST = -2;
create TEMP TABLE m3 as 
select PCN_CD, IND_CD, P_IND_ACH from last_achievement where M_PAST = -3;

create temp TABLE pivot_data as
SELECT a.*,f.ACH_DATE, f.M_FY, b.P_IND_ACH, b.P_IND_ACH as m0, c.P_IND_ACH as m1,d.P_IND_ACH as m2,e.P_IND_ACH as m3, f.M_TO_YE
from head as a 
left join m0  as b on a.PCN_CD = b.PCN_CD and a.IND_CD = b.IND_CD
left join m1  as c on a.PCN_CD = c.PCN_CD and a.IND_CD = c.IND_CD
left join m2  as d on a.PCN_CD = d.PCN_CD and a.IND_CD = d.IND_CD
left join m3  as e on a.PCN_CD = e.PCN_CD and a.IND_CD = e.IND_CD
join (select ACH_DATE, M_FY, M_TO_YE from supp_months_achievement where M_PAST =0) as f;


-- apply patches for null submissions
CREATE temp TABLE pivot_data_patches AS
SELECT a.*,  b.Nm0,b.Nm1,b.Nm2, b.Nm3, NULL as m0_patched,NULL as m1_patched,NULL as m2_patched,NULL as m3_patched
from pivot_data as a left join supp_pcn_achievement_nat_avg as b on a.IND_CD = b.IND_CD; 

-- apply rule 1 for each month
UPDATE pivot_data_patches set m0_patched=m0 where m0 not null;
UPDATE pivot_data_patches set m0_patched=safe_avg(m1,m2,m3,NULL) where m0 is null;
UPDATE pivot_data_patches set m1_patched=m1 where m1 not null;
UPDATE pivot_data_patches set m1_patched=safe_avg(m0,m2,m3,NULL) where m1 is null;
UPDATE pivot_data_patches set m2_patched=m2 where m2 not null;
UPDATE pivot_data_patches set m2_patched=safe_avg(m1,m0,m3,NULL) where m2 is null;
UPDATE pivot_data_patches set m3_patched=m3 where m3 not null;
UPDATE pivot_data_patches set m3_patched=safe_avg(m1,m2,m0,NULL) where m3 is null;
-- apply rule 2
UPDATE pivot_data_patches set m0_patched = Nm0, m1_patched = Nm1, m2_patched = Nm2, m3_patched = Nm3 where m0 is null and m1 is null and m2 is null and m3 is null;

DROP TABLE if EXISTS test_substitutions;
CREATE TABLE test_substitutions as
SELECT * from pivot_data_patches;
-- apply patches
UPDATE pivot_data_patches set m0=m0_patched,m1=m1_patched,m2=m2_patched,m3=m3_patched;


CREATE TEMP TABLE weighted_achievement AS
SELECT ACH_DATE, PCN_CD, IND_CD, M_FY, P_IND_ACH, m0,m1,m2,m3,M_TO_YE,  
min((3*(m0-m1)+2*(m1-m2)+1*(m2-m3))/(3+2+1)*M_TO_YE+m0 ,1) as P_IND_ACH_W, 
(3*(m0-m1)+2*(m1-m2)+1*(m2-m3))/(3+2+1)*M_TO_YE+m0 as P_IND_ACH_W_UNCAPPED 
from pivot_data_patches; 

CREATE TEMP TABLE growth_achievement AS
SELECT a.*,b.C_GROWTH,
min(a.m0*b.C_GROWTH,1) as P_IND_ACH_C, 
a.m0*b.C_GROWTH as P_IND_ACH_C_UNCAPPED, 
b.RULE
from weighted_achievement as a
left join projections as b on a.IND_CD = b.IND_CD and a.M_FY = b.M_FY;


CREATE TEMP TABLE projected_achievement as 
select *,
case 
    when RULE = 'C_GROWTH' then  min(P_IND_ACH_C,1) 
    when RULE = 'W_GROWTH' then  min(P_IND_ACH_W,1)
end as P_IND_ACH_PROJ,
case 
    when RULE = 'C_GROWTH' then  P_IND_ACH_C_UNCAPPED 
    when RULE = 'W_GROWTH' then  P_IND_ACH_W_UNCAPPED
end as P_IND_ACH_PROJ_UNCAPPED
from growth_achievement;

DROP TABLE if EXISTS test_projected_achievement;
CREATE TABLE test_projected_achievement AS
SELECT * from projected_achievement;

DROP TABLE if EXISTS supp_pcn_achievement;
CREATE TABLE supp_pcn_achievement AS
SELECT a.ACH_DATE, a.PCN_CD, a.IND_CD,a.SUBMIT_STATUS,a.NUM,a.DEN, a.P_IND_ACH, b.P_IND_ACH_PROJ,
CASE
    when b.P_IND_ACH_PROJ_UNCAPPED is not NULL then a.DEN * b.P_IND_ACH_PROJ_UNCAPPED
    else NULL
end as NUM_PROJ,
CASE
    when b.P_IND_ACH_PROJ_UNCAPPED is not NULL then a.DEN
    else NULL
end as DEN_PROJ
from actual_achievement as a left join projected_achievement as b on a.ACH_DATE = b.ACH_DATE and a.PCN_CD = b.PCN_CD and a.IND_CD = b.IND_CD;
