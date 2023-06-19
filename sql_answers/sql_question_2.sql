with products as (
    select * from TAKE_HOME_CHALLENGE.ECOMMERCE.PRODUCTS
),

order_items as (
    select * from TAKE_HOME_CHALLENGE.ECOMMERCE.ORDER_ITEMS
),

orders as (
    select * from TAKE_HOME_CHALLENGE.ECOMMERCE.ORDERS
),

base as (
    select
        order_items.ORDER_ITEM_ID,
        order_items.ORDER_ID,
        order_items.PRODUCT_ID,
        order_items.PRICE,

        orders.ORDER_PURCHASE_TIMESTAMP as placed_at,
        orders.ORDER_STATUS,

        products.PRODUCT_CATEGORY_NAME as product_category

    from order_items

    left join products
        on order_items.PRODUCT_ID = products.PRODUCT_ID

    left join orders
        on order_items.ORDER_ID = orders.ORDER_ID

    -- filter to 2017 and only orders that are delivered and final
    where year(placed_at) = 2017
        and orders.ORDER_STATUS = 'delivered'
),

nov_2017_top_3_categories as (
    select top 3
        product_category,

        -- same products exist across multiple order_items rows, so a count(*) is sufficient
        -- e.g. order 00526a9d4ebde463baee25f386963ddc contains 4 of the same items and is 4 rows in order_items
        count(*) as num_sold

    from base

    -- filter to November 2017
    -- could do month(placed_at) = 11 instead, so that this filter wouldn't need updating if we wanted to run
    -- this query for a different calendar year - but that raises the question around the context of why we
    -- are writing this query - was November important (e.g. busiest month for the business), or specifically
    -- November 2017 for whatever reason? Having two date filters forces the person editing the query to think
    -- about this, instead of just updating the base filter and continuing to use November erroneously.
    -- If the base filter is updated but not this one, then this table is empty which will be noticed
    -- TL;DR I don't know the context behind this query so this approach is safer
    where date_trunc('month', placed_at) = '2017-11-01'

    group by 1

    order by 2 desc
),

gmv as (
    select
        date_trunc('week', base.placed_at) as placed_week,
        base.product_category,

        sum(base.price) as total_price,
        sum(total_price) over (partition by base.product_category order by placed_week) as gmv

    -- using this table as the from table implicitly acts as a filter
    from nov_2017_top_3_categories

    left join base
        on nov_2017_top_3_categories.product_category = base.product_category

    group by 1,2
),

final as (
    select
        placed_week,
        product_category,
        gmv,
        gmv::float / lag(gmv) over (partition by product_category order by placed_week) as gmv_growth_rate

    from gmv
)

select * from final order by 1,2

-- esporte_lazer has no sales during the first week of the year meaning we are 'missing' a row
-- for that category in that week. if this is a problem, we can use a date spine containing
-- each week of the year as the base table, resulting in a null price/GMV for that product in
-- the first week (potentially converting to 0 with something like zeroifnull() or coalesce())
