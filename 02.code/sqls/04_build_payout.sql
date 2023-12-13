
-- CALCULATE POINTS ACHIEVEMENT
-- add thresholds
CREATE TEMP table thresholds as 
SELECT a.*, b.NUM, b.DEN, b.PCA, c.P_IND_ACH, c.P_IND_ACH_PROJ,c.NUM_PROJ, c.DEN_PROJ, 
case 
    when a.ACH_DATE<= 20230331 then '22/23'
    else '23/24'
end as FY,
d.IND_LT as LT, d.IND_UT as UT
from (SELECT DISTINCT ACH_DATE, PCN_CD, IND_CD from supp_pcn_data) as a 
left join supp_pcn_data         as b on a.ACH_DATE = b.ACH_DATE and a.PCN_CD = b.PCN_CD and a.IND_CD = b.IND_CD
left join supp_pcn_achievement  as c on a.ACH_DATE = c.ACH_DATE and a.PCN_CD = c.PCN_CD and a.IND_CD = c.IND_CD
left join scheme_info           as d on a.IND_CD = d.PCG_CD;
--  add % points achievement
CREATE TEMP TABLE pts_achievement as 
select *, 
case 
    when P_IND_ACH >= UT then 1
    when P_IND_ACH <= LT then 0
    else (P_IND_ACH-LT)/(UT-LT) 
end as P_PTS_ACH,
case 
    when P_IND_ACH_PROJ is null     then NULL
    when P_IND_ACH_PROJ >= UT       then 1
    when P_IND_ACH_PROJ <= LT       then 0
    else (P_IND_ACH_PROJ-LT)/(UT-LT) 
end as P_PTS_ACH_PROJ
from thresholds; 

-- ADD PAYOUT
-- a) calculate measures for list and prevalence adjustments
CREATE temp TABLE pcn_list AS SELECT PCN_CD, sum(LIST) as PCN_LIST from list GROUP by PCN_CD;
CREATE TEMP TABLE nat_avg_list AS SELECT avg(PCN_LIST) as NAT_AVG_LIST from pcn_list;

CREATE temp TABLE pcn_prev as 
select a.ACH_DATE,a.IND_CD, a.PCN_CD, a.DEN as PREV_NUM, b.PCN_LIST as PREV_DEN 
from pts_achievement as a left join pcn_list as b on a.PCN_CD = b.PCN_CD
where a.DEN is not null;

create TEMP TABLE nat_prev_den as SELECT ACH_DATE,IND_CD, sum(PREV_DEN) as NAT_PREV_DEN from pcn_prev group by ACH_DATE, IND_CD;
create temp TABLE nat_prev_num as SELECT ACH_DATE,IND_CD, sum(PREV_NUM) as NAT_PREV_NUM from pcn_prev group by ACH_DATE,IND_CD;

-- b) update table with measures above
create temp TABLE adjustments_update as 
select a.*, b.PCN_LIST, c.NAT_AVG_LIST,
case when DEN> 0 and b.PCN_LIST >0 then DEN / b.PCN_LIST else NULL end as PCN_PREV,
--case when d.NAT_PREV_NUM>0 then d.NAT_PREV_NUM else 0 end as NAT_PREV_NUM,
--case when e.NAT_PREV_DEN>0 then e.NAT_PREV_DEN else 0 end as NAT_PREV_DEN,
--case when d.NAT_PREV_NUM>0 and e.NAT_PREV_DEN >0 then d.NAT_PREV_NUM / e.NAT_PREV_DEN else 0 end as NAT_AVG_PREV 
d.NAT_PREV_NUM / e.NAT_PREV_DEN as NAT_AVG_PREV 
from pts_achievement as a 
LEFT join pcn_list      as b on a.PCN_CD = b.PCN_CD
join nat_avg_list       as c 
LEFT JOIN nat_prev_num  as d on a.ACH_DATE = d.ACH_DATE and a.IND_CD = d.IND_CD
join nat_prev_den       as e on a.ACH_DATE = e.ACH_DATE and a.IND_CD = e.IND_CD;

UPDATE adjustments_update set PCN_PREV = NAT_AVG_PREV where PCN_PREV is null;
-- b2) calculate adjustments
create temp TABLE adjustments as 
select *, 
case when PCN_LIST>0 and NAT_AVG_LIST>0 then PCN_LIST / NAT_AVG_LIST else 0 end as LIST_ADJ, 
case when PCN_PREV>0 and NAT_AVG_PREV>0 then PCN_PREV/NAT_AVG_PREV else 0 end as PREV_ADJ from adjustments_update; 
-- c) add payout calculations
CREATE temp TABLE payout as 
SELECT a.*, 
a.P_PTS_ACH * b.PTS * a.LIST_ADJ * a.PREV_ADJ * b.PT_VAL as PAYOUT,
0.9 * b.PTS * a.LIST_ADJ * a.PREV_ADJ * b.PT_VAL as BUDGET,
a.P_PTS_ACH_PROJ * b.PTS * a.LIST_ADJ * a.PREV_ADJ * b.PT_VAL as PAYOUT_PROJ
from adjustments as a LEFT JOIN scheme_info as b on a.IND_CD = b.PCG_CD;

-- drop TABLE if EXISTS test_payout;
-- create table test_payout as 
-- select * from payout;

-- ADD ANALYTICS
CREATE TEMP TABLE analytics as 
SELECT *,
case
when P_PTS_ACH is null then -1
when P_PTS_ACH =0    then 0 
when P_PTS_ACH >0 and P_PTS_ACH <0.10 then 1
when P_PTS_ACH <0.20 then 2
when P_PTS_ACH <0.30 then 3
when P_PTS_ACH <0.40 then 4
when P_PTS_ACH <0.50 then 5
when P_PTS_ACH <0.60 then 6
when P_PTS_ACH <0.70 then 7
when P_PTS_ACH <0.80 then 8
when P_PTS_ACH <0.90 then 9
else 10 end as P_IND_PTS_BIN
from payout;


DROP TABLE if EXISTS fact_performance;
CREATE TABLE fact_performance as SELECT * FROM analytics;
