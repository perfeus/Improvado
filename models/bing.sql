-- conversion cost by channel
select sum(spend) / sum(conv) from {{ ref('src_ads_bing_all_data') }};

-- cost per engage


-- impressionss by channel


-- cpc
select sum(spend) / sum(clicks) from {{ ref('src_ads_bing_all_data') }};





select * from  {{ ref('src_ads_bing_all_data') }}
