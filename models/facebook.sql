-- conversation cost by channel
select sum(spend) / sum(purchase) from {{ ref('src_ads_creative_facebook_all_data') }};

-- cost per engage
select sum(spend) / (sum(add_to_cart) + sum(views) + sum(clicks)) 
from {{ ref('src_ads_creative_facebook_all_data') }};

-- impressions by channel
select sum(impressions) from {{ ref('src_ads_creative_facebook_all_data') }};

-- cpc
select sum(spend) / sum(clicks) from {{ ref('src_ads_creative_facebook_all_data') }};



select
    sum(spend) spend,
    sum(add_to_cart) add_to_cart,
    sum(comments) comments,
    sum(comments_2) comments_2,
    sum(likes) likes,
    sum(shares) shares,
    sum(shares_2) shares_2,
    sum(views) views,
    sum(views_2) views_2,
    sum(clicks) clicks,
    sum(clicks_2) clicks_2,
    sum(impressions) impressions,
    sum(mobile_app_install) mobile_app_install,
    sum(inline_link_clicks) inline_link_clicks,
    sum(purchase) purchase,
    sum(purchase_2) purchase_2,
    sum(complete_registration) complete_registration,
    sum(purchase_value) purchase_value
from {{ ref('src_ads_creative_facebook_all_data') }};




select * from {{ ref('src_ads_creative_facebook_all_data') }}