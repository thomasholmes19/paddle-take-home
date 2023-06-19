/* Notes:

   Assume order counts which define an 'active seller' to mean unique orders placed, not order items
   e.g. if a seller receives an order for 1 each of products X and Y, this only counts as 1 order (but is 2 rows in order_items)

   Datagrip is suggesting to replace the case statements with IFF functions which seems to be a Snowflake function
   that I'm not used to from Redshift, but I'll stick with case statements as they are more universal (and in this
   case functionally equivalent)

   This query is quite long, but has been written with dbt in mind so could refactor parts into ephemeral models
   or intermediary models upstream

   The main benefit of writing a long step-by-step query like this is that each CTE can be individually queried
   for sense checking, debugging and testing. It's definitely possible to write something shorter, but it wouldn't
   be as readable or dev-friendly

   There's space to adhere more to DRY in this query, but given the _sellers -> _stats logic is only repeated twice
   it's more readable in my opinion. I usually try and avoid jinja loops to generate CTEs in queries like this as
   they can be quite hard to read and understand later.
       - (IMO, DRY in sql/dbt should in most cases only be applied to refactoring models, e.g. weekly_sellers could
          be used in multiple downstream models, rather than trying to avoid repeating code/logic in one model)
 */

with orders as (
    select * from TAKE_HOME_CHALLENGE.ECOMMERCE.ORDERS
),

order_items as (
    select * from TAKE_HOME_CHALLENGE.ECOMMERCE.ORDER_ITEMS
),

base as (
    -- distinct so have unique permutations of order_id and seller_id
    -- placed_at is left joined so will not cause extra rows
    select distinct
        order_items.ORDER_ID as order_id,
        order_items.SELLER_ID as seller_id,
        orders.ORDER_PURCHASE_TIMESTAMP as placed_at

    from order_items

    left join orders
        on order_items.ORDER_ID = orders.ORDER_ID

    -- filter to 2017 and only orders that are delivered and final
    where year(placed_at) = 2017
        and orders.ORDER_STATUS = 'delivered'

),

-- daily_sellers is a bit redundant here as seller_id comes from order_items meaning the
-- seller_id appears if and only if they have sold an order_item.
-- however writing like this allows us to change the definition of a daily active seller
-- easily and also add on other daily stats at a later date if we want to extend this model
daily_sellers as (
    select
        date_trunc('day', placed_at) as placed_date,
        seller_id,

        count(*) as orders_received,
        case when orders_received > 0 then 1 else 0 end as is_active_seller

    from base
    group by 1,2
),

daily_stats as (
    select
        placed_date,
        date_trunc('month', placed_date) as placed_month,
        sum(is_active_seller) as num_active_sellers

    from daily_sellers
    group by 1
),

-- weeks do not perfectly aggregate into months due to calendar weeks crossing over 2 calendar
-- months, but will just use start of week from date_trunc for simplicity
weekly_sellers as (
    select
        seller_id,
        date_trunc('week', placed_at) as placed_week,

        count(*) as orders_received,
        case when orders_received >= 5 then 1 else 0 end as is_active_seller

    from base
    group by 1,2
),

weekly_stats as (
    select
        placed_week,
        date_trunc('month', placed_week) as placed_month,
        sum(is_active_seller) as num_active_sellers

    from weekly_sellers
    group by 1
),

monthly_sellers as (
    select
        date_trunc('month', placed_at) as placed_month,
        seller_id,

        count(*) as orders_received,
        case when orders_received >= 25 then 1 else 0 end as is_active_seller

    from base
    group by 1,2
),

monthly_stats as (
    select
        placed_month,
        sum(is_active_seller) as num_active_sellers

    from monthly_sellers
    group by 1
),

-- we could save a CTE by using monthly_stats as the final CTE, but I think it'd be better
-- to keep the expected structure and pattern of CTEs that the reader would be expecting at
-- this point. I don't feel too strongly about it either way though so wouldn't mind changing
-- if asked in a code review
final as (

    select
        monthly_stats.placed_month,

        -- max() because not in group by, but grain is unchanged so do not lose any data
        max(monthly_stats.num_active_sellers) as monthly_active_sellers,
        avg(weekly_stats.num_active_sellers) as avg_weekly_active_sellers,
        avg(daily_stats.num_active_sellers) as avg_daily_active_sellers

    from monthly_stats

    left join weekly_stats
        on monthly_stats.placed_month = weekly_stats.placed_month

    left join daily_stats
        on monthly_stats.placed_month = daily_stats.placed_month

    group by 1

)

select * from final order by placed_month desc;
