-- 1. 

select * 
from streams s
where s.new_timestamp >= (NOW() - INTERVAL '1 hours' );

-- 2. 

select s.client_ip, count(s.client_ip)
from streams s
where s.new_timestamp >= (NOW() - INTERVAL '1 hours' )
group by s.client_ip;

-- 3. 

select sum(s.sent_bytes)
from streams s
where s.new_timestamp >= (NOW() - INTERVAL '1 hours' );

-- 4.

select s.backend_status_code, count(s.backend_status_code)
from streams s
where s.new_timestamp >= (NOW() - INTERVAL '1 hours' )
group by s.backend_status_code
order by s.backend_status_code;

-- 5.

create or replace view request_uri_stripped as 
    select 
        s.request_uri, 
        s.new_timestamp ,
        split_part(split_part(s.request_uri,'//',2), '/', 2) 
    from streams s; 

select 
  split_part,
  count(split_part)
from request_uri_stripped
where new_timestamp >= (NOW() - INTERVAL '123213 hours' )
group by split_part;
