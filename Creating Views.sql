-- Views
-- airbnbdb.view_calendar_group source
create or replace
algorithm = UNDEFINED view `airbnbdb`.`view_calendar_group` as
select
    `c`.`listing_id` as `listing_id`,
    `c`.`price` as `price`,
    min(str_to_date(`c`.`date`, '%Y-%m-%d')) as `min(STR_TO_DATE(``date``, '%Y-%m-%d'))`,
    max(str_to_date(`c`.`date`, '%Y-%m-%d')) as `max(STR_TO_DATE(``date``, '%Y-%m-%d'))`
from
    `airbnbdb`.`calendar` `c`
group by
    `c`.`listing_id`,
    `c`.`price`;


-- airbnbdb.view_calendar_march23may24 source
create or replace
algorithm = UNDEFINED view `airbnbdb`.`view_calendar_march23may24` as
select
    distinct str_to_date(`airbnbdb`.`calendar`.`date`,
    '%Y-%m-%d') as `date`
from
    `airbnbdb`.`calendar`;


-- airbnbdb.view_popularity_location_time source
create or replace
algorithm = UNDEFINED view `airbnbdb`.`view_popularity_location_time` as
select
    count(`rd`.`listing_id`) as `count_listing_id`,
    substring_index(`rd`.`date`, '-', 2) as `year_month`,
    `ld`.`city` as `city`,
    `ld`.`state` as `state`
from
    (`airbnbdb`.`reviews_detailed` `rd`
join `airbnbdb`.`listings_detailed` `ld` on
    ((`rd`.`listing_id` = `ld`.`id`)))
group by
    `year_month`,
    `ld`.`city`,
    `ld`.`state`;