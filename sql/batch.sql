-- 1. 
select p.remote_addr, count(p.remote_addr)
from parsed p 
group by p.remote_addr;

-- 2.

select 
	extract (hour from date_trunc('hour', p.time_local)) as req_hour,
	count(*)
from parsed p
group by req_hour
order by req_hour;

-- 3.

select p.method, count(p.method)
from parsed p 
group by p.method;

-- 4.

select 
	count(*) as cnt,
	100 * (count(*) * 1.0 / sum(count(*)) over (partition by a.nr)),
	a.elem,
	a.nr as level
	-- split_part(p.path, '?', 1) as parsed_path
from parsed p
left join lateral string_to_table(split_part(p.path, '?', 1), '/') with ordinality as a(elem, nr) on true
group by level, a.elem
order by level asc, cnt desc;

-- 5.

select p.os, count(p.os)
from parsed p 
group by p.os;

-- 6.

select p.browser_family, count(p.browser_family)
from parsed p 
group by p.browser_family;

-- 7.

select p.status, count(p.status)
from parsed p 
group by p.status
order by p.status;

-- 8.
-- ovo moze da se nadogradi tako sto se uzima samo host address od referera bez putanje
select p.http_referer, count(p.http_referer)
from parsed p 
group by p.http_referer;

-- 9.

select 
    sum(p.body_bytes_sent) / 1024 as data_kb, 
    p.path
from parsed p 
group by p.path
order by sum(p.body_bytes_sent) desc;

-- 10.

select sum(p.body_bytes_sent), p.remote_addr
from parsed p 
group by p.remote_addr
order by sum(p.body_bytes_sent) desc;

-- 11. 

-- windowed upit koji za cilj ima da nadje top 3 zahteva sa najduzim odgovorom za svaki http referer

SELECT referer, bytes_sent, pos
FROM
  (SELECT split_part(split_part(p.http_referer,'//',2), '/', 1) as referer, p.body_bytes_sent as bytes_sent,
		rank() OVER (PARTITION BY split_part(split_part(p.http_referer,'//',2), '/', 1) ORDER BY p.body_bytes_sent DESC) AS pos
     FROM parsed p
  ) AS ss
WHERE pos <= 3;
