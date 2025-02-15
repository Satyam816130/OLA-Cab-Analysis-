create database ola_rides 
use ola_rides


create table ola(
date date,
time time,
booking_id nvarchar(100),
booking_status varchar(40),
customer_id nvarchar(50),
vechile_type varchar(20),
pickup_location nvarchar(50),
drop_location nvarchar(50),
v_tat float,
c_tat float,
cancelled_rides_by_customer varchar(max),
cancelled_rides_by_driver varchar(max),
incomplete_rides varchar(15),
incomplete_rides_reason varchar(50),
booking_value float,
payment_method varchar(20),
ride_distance float,
driver_rating float,
customer_rating float,
vechile_image nvarchar(50))



insert into ola(date,time,booking_id,booking_status,customer_id,vechile_type,pickup_location,drop_location,v_tat,c_tat,cancelled_rides_by_customer,
cancelled_rides_by_driver,incomplete_rides,incomplete_rides_reason,booking_value,payment_method,ride_distance,driver_rating,
customer_rating,vechile_image)
select date,time,Booking_ID,Booking_Status,Customer_ID,Vehicle_Type,Pickup_Location,Drop_Location,V_TAT,C_TAT,Canceled_Rides_by_Customer,
Canceled_Rides_by_Driver,Incomplete_Rides,Incomplete_Rides_Reason,Booking_Value,Payment_Method,Ride_Distance,Driver_Ratings,
Customer_Rating,[Vehicle Images]
from dbo.july$

select*from ola

--retrieve all successfull bookings.

create index booking on ola(booking_status)
create index types on  ola( vechile_type)

select*from ola
where booking_status='success'

--find average ride distance of each vechile.

select vechile_type, round(avg(ride_distance),2) as avg_distance
from ola
group by vechile_type

--find the total percenatge of customer ride cancel.

select*from ola

with cte as(
select count(booking_id) as total_booking 
from ola)
, cte2 as(
select count(booking_id)as cancel_booking from ola
where booking_status='canceled by customer')

select cast(c2.cancel_booking*100/c.total_booking as decimal(5,2)) as percenatge_cancellation_customer
from cte c
cross join cte2 c2

--find the most, medium and least used vechile type

select*from ola

with cte as(
select vechile_type , count(booking_id) as cnt
from ola 
group by vechile_type),

cte2 as(
select vechile_type, cnt, row_number() over(order by cnt desc) as desc_rnk,row_number() over(order by cnt asc) as asc_rnk
from cte ),

mediumm as(
select vechile_type, avg(cnt ) as cnt 
from cte2 
where asc_rnk=desc_rnk or asc_rnk+1=desc_rnk 
group by vechile_type ),

most as(
select * from(
select *, ROW_NUMBER() over(order by cnt desc) ranks
from cte ) e
where e.ranks=1),

leasts as(
select * from(
select *, ROW_NUMBER() over(order by cnt asc) ranks
from cte ) e
where e.ranks=1)

select vechile_type,'most used' as vechile_type_used 
from most 
union all 
select vechile_type, 'medium used'as vechile_type_used 
from mediumm 
union all
select vechile_type,'least used' as vechile_type_used 
from leasts 

--find the highest booking value and the distance.

select top 1 ride_distance, booking_value as max_booking_value
from ola
order by max_booking_value desc

--find the total business day bookings of current week.


select count(booking_id) as businessday_bookings
from ola
where DATEPART(week,date)=GETDATE()
and DATEPART(WEEKDAY,date) in(2,3,4,5,6)

--find the pickup location where the rider cancelled the most ride.


select top 1 pickup_location, count(cancelled_rides_by_driver) as cnt
from ola
group by pickup_location
order by cnt desc 

-- find the drop location and time when the rider get less rating .

 
select top 1 drop_location, time,customer_rating as rating
from ola
where customer_rating is not null 
order by rating asc 

-- Detect the anomalities in daily ride booking value based on deviations from from 7 days moving average.

select * from ola 

with cte as(
	select datename(WEEKDAY,date) as trip_date,
			sum(booking_value) as total_amount
	from ola 
	group by datename(WEEKDAY,date) ),

cte2 as(
	select trip_date,total_amount, avg(total_amount) over(order by trip_date rows between 6 preceding and current row) as mvg_avg
	from cte )

select c.trip_date,c.total_amount,abs(c.total_amount-c2.mvg_avg) as deviation 
from cte c
join cte2 c2
on c.trip_date=c2.trip_date 
where abs(c.total_amount-c2.mvg_avg)>(c2.mvg_avg*0.1) --- 10% standard 


--Calculate the churn rate of customers in last month where churn can be defined as customers
--who havent take any ride in last 30 days.

select*from ola

with all_customers as(
	select distinct customer_id
	from ola),

active_customers as(
	select distinct customer_id
	from ola
	where date>=DATEADD(day,-30,(select max(date) from ola))),

churn_customers as(
	select distinct al.customer_id
	from all_customers al
	left join active_customers ac
	on al.customer_id=ac.customer_id
	where ac.customer_id is null),

total_customer as(
	select count(distinct customer_id) as total_cust_cnt
	from all_customers),

total_churn_customer as(
	select count(distinct customer_id) as churn_cust_cnt
	from churn_customers)

select (tcc.churn_cust_cnt*100/tc.total_cust_cnt) as churn_rate
from total_customer tc 
cross join total_churn_customer tcc

-- compare business days bookings with non business days bookings.
select*from ola

with business_days as(
select  count(booking_id) as cnt_business_day
from ola
where DATENAME(WEEKDAY, date) in('monday','tuesday','wednesday','thursday','friday')
),

weekend_days as(
select  count(booking_id) as cnt_weekend_day
from ola
where DATENAME(WEEKDAY, date) in('saturday','sunday')
)
,
total_booking as(
	select count(booking_id)as total_bookings from ola )

select  t.total_bookings, (b.cnt_business_day*100/t.total_bookings) as businessday_perc,
(w.cnt_weekend_day*100/t.total_bookings) as weekend_booking_perc
from total_booking t
cross join business_days b
cross join weekend_days w

select* from ola 

---find the customer who booked a ride for consecutive day.

with cte as(
	select distinct customer_id, booking_id,date,lead(date) over(partition by customer_id order by date) as next_date
	from ola )
select distinct customer_id 
from cte 
where DATEDIFF(day,date,next_date)=1


