-- coversions cost by channel
select sum(spend) / sum(url_clicks) from {{ ref('src_promoted_tweets_twitter_all_data') }};
select sum(spend) / sum(video_total_views) from {{ ref('src_promoted_tweets_twitter_all_data') }};

-- cost per engage
select sum(spend) / sum(engagements) from {{ ref('src_promoted_tweets_twitter_all_data') }};

-- impressions by channel
select sum(impressions) from {{ ref('src_promoted_tweets_twitter_all_data') }};

-- cpc
select sum(spend) / sum(clicks) from {{ ref('src_promoted_tweets_twitter_all_data') }};



select * from {{ ref('src_promoted_tweets_twitter_all_data') }}