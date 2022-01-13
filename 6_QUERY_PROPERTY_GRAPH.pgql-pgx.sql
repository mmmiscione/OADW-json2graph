SELECT s.STATIONS_CODE as STATION_CODE, 
       s.STATIONS_NAME as STATION_NAME, 
       max(in_degree(s) + out_degree(s)) as max_num_arr_dep,
       avg(b.DEP_MINUTES - a.ARR_MINUTES) as avg_conn_time,
       count(a.TRAIN_NUMBER + b.TRAIN_NUMBER) as num_trains
FROM MATCH () -[a]-> (s) -[b]->() ON TRAIN_SCHEDULE_GRAPH
where a.STATION_CODE != b.STATION_ARRIVAL_CODE
  and b.DEP_MINUTES - a.ARR_MINUTES > ${min_time}
  and b.DEP_MINUTES - a.ARR_MINUTES < ${min_time}+${delta}
group by s.STATIONS_CODE, s.STATIONS_NAME
order by max_num_arr_dep desc
limit 100


SELECT distinct a.STATIONS_CODE as origin,
       LISTAGG(distinct e.train_number, ' , ') as train_hops, 
       count(distinct e.train_number) as num_cambi,
       LISTAGG(distinct e.STATION_CODE, ' , ') as station_hops,
       b.STATIONS_CODE as destin,
       LISTAGG(e.DURATION_SEC, ' , ') as duration_sequence, 
       SUM(e.DURATION_SEC)/60 AS tot_duration_min
  FROM MATCH TOP 3 SHORTEST (a:station) - [e:connection] ->* (b:station) ON TRAIN_SCHEDULE_GRAPH
 WHERE a.stations_code = '${station_to=PRGD}'  AND b.stations_code =  '${station_HUB= KOTR,KOTR|GWYR|TYMR|INDR|BRST|KTBR|MACR|JRPD}'
 ORDER BY tot_duration_min


SELECT a.STATIONS_CODE as origin,
       b.STATIONS_CODE as destin,
       SUM(e.DURATION_SEC)/60 AS tot_dur_min, 
       count(distinct e.train_number) as num_cambi,
       LISTAGG(distinct e.train_number, ' + ') as train_hops,
       LISTAGG(distinct e.STATION_CODE, ' , ') as station_hops,
       SUM(e.DURATION_SEC) AS tot_duration, 
       LISTAGG(e.DURATION_SEC, ' , ') as duration_sequence 
  FROM MATCH CHEAPEST ( (a:station) (-[e:connection]-> COST e.DURATION_SEC)* (b:station) ) ON TRAIN_SCHEDULE_GRAPH
 WHERE a.stations_code = '${station_to=PRGD,PRGD}' AND b.stations_code = '${station_HUB= KOTR,KOTR|GWYR|TYMR|INDR|KTBR|BRST|KTBR|MACR}'
 ORDER BY tot_duration