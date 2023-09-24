-- Missing values
select count(listing_id) from calendar c where isnull(listing_id);
select count(`date`) from calendar c where isnull(listing_id);
select count(available) from calendar c where isnull(listing_id);
select count(price) from calendar c where isnull(listing_id);
select count(minimum_nights) from calendar c where isnull(listing_id);
select count(maximum_nights) from calendar c where isnull(listing_id);


-- Views
create view view_calendar_group as (
	select 
		listing_id
		, price
		, min(STR_TO_DATE(`date`, '%Y-%m-%d'))
		, max(STR_TO_DATE(`date`, '%Y-%m-%d')) 
	from calendar c 
	group by listing_id, price
);

create view view_calendar_march23may24 as (
	select distinct STR_TO_DATE(date, '%Y-%m-%d') as `date` 
	from calendar
);
