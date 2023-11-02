-- conversion cost by channel
select sum(spend) / sum(conversions) from {{ ref('src_ads_tiktok_ads_all_data') }};

-- cost per engage


-- impressions by channel
select sum(impressions) from {{ ref('src_ads_tiktok_ads_all_data') }};

-- cpc
select sum(spend) / sum(clicks) from {{ ref('src_ads_tiktok_ads_all_data') }};



select * from {{ ref('src_ads_tiktok_ads_all_data') }}