CREATE PROPERTY GRAPH TRAIN_SCHEDULE_GRAPH
  VERTEX TABLES (
    vw_stations as station
      KEY ( stations_code )
      PROPERTIES ( stations_address, stations_code, stations_lat, stations_lon, stations_name )
  )
  EDGE TABLES (
    vw_hops as connection
      KEY ( id )
      SOURCE KEY ( station_code ) REFERENCES station
      DESTINATION KEY ( station_arrival_code ) REFERENCES station
      PROPERTIES ( arr_minutes, day, new_day, departure, departure_date, dep_minutes, duration_sec, id, new_arrival, new_arrival_date, station_arrival_code, station_arrival_name, station_code, station_name, train_name, train_number )
  )

