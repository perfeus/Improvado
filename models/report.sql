with bing as (
    select
        ad_id, 
        0 as add_to_cart, 
        adset_id,
        campaign_id,
        channel,
        clicks,
        null as comments,
        null as creative_id, 
        date,
        0 as engagements,
        imps as impressions,
        0 as installs,
        0 as likes,
        0 as link_clicks,
        null as placement_id,
        0 as post_click_conversions,
        0 as post_view_conversions,
        0 as post,
        0 as purchase,
        0 as registrations,
        revenue,
        0 as shares,
        spend,
        conv as total_conversions,
        0 as video_views
    from {{ ref('src_ads_bing_all_data') }}
),

facebook as (
    select
        ad_id, 
        add_to_cart, 
        adset_id,
        campaign_id,
        channel,
        clicks,
        comments,
        creative_id, 
        date,
        add_to_cart + views + clicks as engagements,
        impressions,
        mobile_app_install as installs,
        likes,
        inline_link_clicks as link_clicks,
        null as placement_id,
        0 as post_click_conversions,
        0 as post_view_conversions,
        0 as post,
        purchase,
        complete_registration as registrations,
        0 as revenue,
        shares,
        spend,
        purchase as total_conversions, 
        views as video_views
    from {{ ref('src_ads_creative_facebook_all_data') }}
),

tiktok as (
    select
        ad_id, 
        add_to_cart, 
        null as adset_id,
        campaign_id,
        channel,
        clicks,
        null as comments,
        null as creative_id, 
        date,
        0 as engagements,
        impressions,
        rt_installs + skan_app_install as installs,
        0 as likes,
        0 as link_clicks,
        null as placement_id,
        0 as post_click_conversions,
        0 as post_view_conversions,
        0 as post,
        purchase,
        registrations,
        0 as revenue,
        0 as shares,
        spend, 
        conversions as total_conversions,
        video_views
    from {{ ref('src_ads_tiktok_ads_all_data') }}
),

twitter as (
    select
        null as ad_id, 
        0 as add_to_cart, 
        null as adset_id,
        campaign_id,
        channel,
        clicks,
        comments,
        null as creative_id, 
        date,
        engagements,
        impressions,
        0 as installs,
        likes,
        url_clicks as link_clicks,
        null as placement_id,
        0 as post_click_conversions,
        0 as post_view_conversions,
        0 as post,
        0 as purchase,
        0 as registrations,
        0 as revenue,
        0 as shares,
        spend,
        0 as total_conversions, 
        video_total_views as video_views
    from {{ ref('src_promoted_tweets_twitter_all_data') }}
),

final as (
    select * from bing
    union all
    select * from facebook
    union all
    select * from tiktok
    union all 
    select * from twitter
)

select * from final