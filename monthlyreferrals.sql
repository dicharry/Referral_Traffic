(WITH articles AS (SELECT
DISTINCT
visit_referrer,
#timestamp(date_time) as read_time,
safe_cast(date_time as timestamp) as read_time,
IF(REGEXP_CONTAINS(page_url, 'utm_medium=news_tab') AND NET.REG_DOMAIN(visit_referrer) = 'facebook.com', 1, 0) as news_tab,
IF(REGEXP_CONTAINS(post_evar56, 'personalized'), 1, 0) as locked,
CASE WHEN post_page_event = '0' AND (CAST(exclude_hit AS INT64) <= 0 AND hit_source NOT IN ('5', '8', '9')) AND prop1 = "Article" THEN concat(post_visid_high, post_visid_low, visit_num, visit_page_num) ELSE NULL END as views,
IF(post_prop27 IN ('WSJ_sub_yes'), 1, 0) as sub_status,
(CASE WHEN prop1 = "Article" THEN CONCAT(post_visid_high, post_visid_low) END) as unique_id,
visit_num,
#IF(prop1 = 'Article', 1, 0) as article,
IF(post_page_event = "50",1,0) as video_play,
visit_page_num,
-- IF(REGEXP_CONTAINS(visit_start_pagename, 'Article'), 1, 0) as article_visit,
-- IF(post_video <> " ", 1, 0) as video_visit,
IF(page_event_var2 = 'WSJ_Article_social_share', 1, 0) as share,
prop10
FROM `djomniture.cipomniture_djglobal.*`
WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^\d{4}$*')
AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -14 MONTH), MONTH)
-- AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
and (lower(post_prop8) NOT LIKE 'dwsjn%' and (lower(post_evar11) LIKE '%online journal%' or lower(post_evar11) LIKE '%wsj%' or lower(post_prop24) = 'the wall street journal'))
#and prop1 = "Article"
),
--AND (REGEXP_CONTAINS(page_url, 'wsj[.com|.net]')
--OR channel IN ('Online Journal'))),
sales as (SELECT
CONCAT(post_visid_high, post_visid_low) as unique_id,
timestamp(date_time) as subscribe_time,
post_evar39 as conversion_headline,
visit_num,
post_purchaseid,
FROM `djomniture.cipomniture_djglobal.*`
WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^\d{4}$*')
AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -14 MONTH), MONTH)
-- AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
AND SAFE_CAST(exclude_hit AS INT64) <= 0
AND hit_source NOT IN ('5','8','9')
AND REGEXP_CONTAINS(post_event_list, r'^1,|,1,|,1$')
--Relevant order filters go here (i.e. excluding student)
AND REGEXP_CONTAINS(page_url, 'store.wsj.com|buy.wsj.com')
AND channel IN ('Online Journal'))

SELECT
CASE
      #WHEN REGEXP_CONTAINS(page_url, 'utm_medium=news_tab') AND NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook News'
      WHEN news_tab = 1 THEN "Facebook News Tab"
      WHEN prop10 IN ('WSJ_wsj_RHF')#,'BOL_bar_RHF')
        OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'robinhood') THEN 'Robinhood'
      WHEN REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'goog|android.gm') THEN 'Google'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'ampproject.org' THEN 'Google'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'newsbreakapp.com' THEN 'NewsBreak'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook Main Feed'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'investors.com' THEN 'IBD'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'mansionglobal.com' THEN 'Mansion Global'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'realclearpolitics.com' THEN 'RealClearPolitics'
      WHEN NET.REG_DOMAIN(visit_referrer) = 't.co'
        OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'twitter') THEN 'Twitter'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'linkedin.com' OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'lnkd') OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'linked') THEN 'LinkedIn'
      WHEN visit_referrer LIKE "%finance.yahoo%" THEN 'Yahoo Finance'
      WHEN REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'yahoo') THEN 'Yahoo'
      WHEN NET.REG_DOMAIN(visit_referrer) IS NULL OR NET.REG_DOMAIN(visit_referrer) = 'wsj.com' THEN 'Direct'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'apple.news' THEN 'Apple News'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook Main Feed'
      #WHEN NET.REG_DOMAIN(visit_referrer) = 'News Tab' THEN 'FB News Tab'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'smartnews.com' THEN 'SmartNews'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'snapchat.com' THEN 'Snapchat'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'apple.com' THEN 'Apple Stocks'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'marketwatch.com' THEN 'MarketWatch'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'barrons.com' THEN "Barron's"
      WHEN NET.REG_DOMAIN(visit_referrer) = 'bing.com' THEN 'Bing'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'msn.com' THEN 'MSN'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'reddit.com' THEN 'Reddit'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'instagram.com' THEN 'Instagram'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'flipboard.com' THEN 'Flipboard'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'youtube.com' THEN 'YouTube'
      WHEN prop10 LIKE '%cashapprss%' THEN 'Cash App'
      WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%drudge%' THEN 'Drudge Report'
      WHEN prop10 LIKE '%wsj_taboola_rss%' THEN 'Taboola News'
      #WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%mktwch_taboola_rss%' THEN 'Taboola News'
      #WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%bar_taboola_rss%' THEN 'Taboola News' #this can probably be shortened to %taboola_rss%
      WHEN prop10 LIKE '%WSJ_Euronews%' THEN 'Euronews'
      #WHEN REGEXP_CONTAINS(visit_referrer, 'square') THEN "Square"
      WHEN NET.REG_DOMAIN(articles.visit_referrer) IS NULL THEN 'wsj.com'
    ELSE
    NET.REG_DOMAIN(visit_referrer) END as referrer,
"WSJ" as brand,
#CASE WHEN articles.prop10 IN ('MW_mw_RHF','_mw_RHF','WSJ_wsj_RHF','BOL_bar_RHF','cashapprss','cashapprss','WSJ_WSJ_Euronews','WSJ_wsj_square','_wsj_square','WSJ_wsj_taboola_rss','MW_mktwch_taboola_rss','BOL_bar_taboola_rss','WSJ_cashapprss','BOL_cashapprss') THEN prop10
#WHEN news_tab = 1 THEN "News Tab"
#WHEN NET.REG_DOMAIN(articles.visit_referrer) IS NULL THEN 'wsj.com'
#ELSE NET.REG_DOMAIN(articles.visit_referrer) END as referrer,
SAFE_CAST(CONCAT(SUBSTR(SAFE_CAST(EXTRACT(date FROM read_time) as STRING),1,8),'01') as DATE) as month,
-- (CASE WHEN articles.article = 1 THEN "article"
-- WHEN articles.video_play = 1 THEN "video_plays" END) as content_type,
SUM(articles.video_play) as video_plays,
COUNT(DISTINCT articles.unique_id) as uniques,
COUNT(DISTINCT articles.views) as views,
COUNT(DISTINCT CONCAT(articles.unique_id, articles.visit_num)) as visits,
COUNT(DISTINCT IF(articles.visit_num = sales.visit_num, post_purchaseid, null)) as orders_same_visit,
COUNT(DISTINCT post_purchaseid) as orders_7_days,
COUNT(DISTINCT post_purchaseid) as purchases,
COUNT(DISTINCT articles.unique_id) as unique_ids,
COUNT(DISTINCT post_purchaseid) / COUNT(DISTINCT articles.unique_id) as conversion_rate
FROM
articles
LEFT JOIN
sales ON
articles.unique_id = sales.unique_id
AND articles.read_time < sales.subscribe_time
AND DATE_DIFF(DATE(sales.subscribe_time), DATE(articles.read_time), DAY) < 7

GROUP BY
referrer,
month
having uniques > 1000
#and content_type IS NOT NULL

ORDER BY
uniques DESC)










UNION ALL










(WITH articles AS (SELECT
DISTINCT
visit_referrer,
#timestamp(date_time) as read_time,
safe_cast(date_time as timestamp) as read_time,
IF(REGEXP_CONTAINS(page_url, 'utm_medium=news_tab') AND NET.REG_DOMAIN(visit_referrer) = 'facebook.com', 1, 0) as news_tab,
IF(REGEXP_CONTAINS(post_evar56, 'personalized'), 1, 0) as locked,
CASE WHEN post_page_event = '0' AND (CAST(exclude_hit AS INT64) <= 0 AND hit_source NOT IN ('5', '8', '9')) AND prop1 = "Article" THEN concat(post_visid_high, post_visid_low, visit_num, visit_page_num) ELSE NULL END as views,
IF(post_prop27 IN ('BOL_sub_yes'), 1, 0) as sub_status,
(CASE WHEN prop1 = "Article" THEN CONCAT(post_visid_high, post_visid_low) END) as unique_id,
visit_num,
#IF(prop1 = 'Article', 1, 0) as article,
IF(post_page_event = "50",1,0) as video_play,
visit_page_num,
IF(REGEXP_CONTAINS(visit_start_pagename, 'Article'), 1, 0) as article_visit,
IF(post_video <> " ", 1, 0) as video_visit,
IF(page_event_var2 = 'BOL_Article_social_share', 1, 0) as share,
prop10
FROM `djomniture.cipomniture_djglobal.*`
WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^\d{4}$*')
AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -14 MONTH), MONTH)
-- AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
#BARRONS SEGMENT
and ((lower(channel) like '%barron%' or lower(channel) = 'bol') or lower(post_prop24) = 'barron s' 
or lower(post_prop1) = 'barrons us' or lower(post_prop1) = 'barrons emea' or lower(post_prop1) = 'barrons apac' 
or lower(post_prop5) = 'www.barrons.com/' or (lower(post_prop5) not like 'customcenter' and channel <> "" and ((lower(user_server) like '%www.%'
 and lower(user_server) like '%barrons.com%') or lower(user_server) = 'online.barrons.com' or (lower(user_server) like '%barron%' 
 and lower(user_server) like '%newsmemory.com%') or (lower(post_prop5) like '%www.%' and lower(post_prop5) like '%barrons.com%'))) OR lower(page_url) like "%www.barrons.com%")

#and prop1 = "Article"
),
--AND (REGEXP_CONTAINS(page_url, 'wsj[.com|.net]')
--OR channel IN ('Online Journal'))),
sales as (SELECT
CONCAT(post_visid_high, post_visid_low) as unique_id,
timestamp(date_time) as subscribe_time,
post_evar39 as conversion_headline,
visit_num,
post_purchaseid,
FROM
`djomniture.cipomniture_djglobal.*`
WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^\d{4}$*')
AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -14 MONTH), MONTH)
-- AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
AND SAFE_CAST(exclude_hit AS INT64) <= 0
AND hit_source NOT IN ('5',
'8',
'9')
AND REGEXP_CONTAINS(post_event_list, r'^1,|,1,|,1$')
--Relevant order filters go here (i.e. excluding student)
AND REGEXP_CONTAINS(page_url, 'store.barrons.com|buy.barrons.com')
AND channel IN ('Barrons Online', 'Barrons'))

SELECT
CASE
      #WHEN REGEXP_CONTAINS(page_url, 'utm_medium=news_tab') AND NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook News'
      WHEN news_tab = 1 THEN "Facebook News Tab"
      WHEN prop10 IN ('BOL_bar_RHF')#'MW_mw_RHF','_mw_RHF','WSJ_wsj_RHF',)
        OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'robinhood') THEN 'Robinhood'
      WHEN REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'goog|android.gm') THEN 'Google'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'ampproject.org' THEN 'Google'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'investors.com' THEN 'IBD'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'newsbreakapp.com' THEN 'NewsBreak'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook Main Feed'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'mansionglobal.com' THEN 'Mansion Global'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'realclearpolitics.com' THEN 'RealClearPolitics'
      WHEN NET.REG_DOMAIN(visit_referrer) = 't.co'
        OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'twitter') THEN 'Twitter'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'linkedin.com' OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'lnkd') OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'linked') THEN 'LinkedIn'
      WHEN visit_referrer LIKE "%finance.yahoo%" THEN 'Yahoo Finance'
      WHEN REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'yahoo') THEN 'Yahoo'
      WHEN NET.REG_DOMAIN(visit_referrer) IS NULL OR NET.REG_DOMAIN(visit_referrer) = 'barrons.com' THEN 'Direct'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'apple.news' THEN 'Apple News'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook Main Feed'
      #WHEN NET.REG_DOMAIN(visit_referrer) = 'News Tab' THEN 'FB News Tab'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'smartnews.com' THEN 'SmartNews'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'snapchat.com' THEN 'Snapchat'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'apple.com' THEN 'Apple Stocks'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'marketwatch.com' THEN 'MarketWatch'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'barrons.com' THEN "Barron's"
      WHEN NET.REG_DOMAIN(visit_referrer) = 'wsj.com' THEN 'Wall Street Journal'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'bing.com' THEN 'Bing'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'msn.com' THEN 'MSN'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'reddit.com' THEN 'Reddit'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'instagram.com' THEN 'Instagram'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'flipboard.com' THEN 'Flipboard'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'youtube.com' THEN 'YouTube'
      WHEN prop10 LIKE '%cashapprss%' THEN 'Cash App'
      WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%drudge%' THEN 'Drudge Report'
      #WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%wsj_taboola_rss%' THEN 'Taboola News'
      #WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%mktwch_taboola_rss%' THEN 'Taboola News'
      WHEN prop10 LIKE '%bar_taboola_rss%' THEN 'Taboola News' #this can probably be shortened to %taboola_rss%
      #WHEN prop10 LIKE '%WSJ_Euronews%' THEN 'Euronews'
      #WHEN REGEXP_CONTAINS(visit_referrer, 'square') THEN "Square"
      WHEN NET.REG_DOMAIN(articles.visit_referrer) IS NULL THEN 'barrons.com'
    ELSE
    NET.REG_DOMAIN(visit_referrer) END as referrer,
"Barron's" as brand,
SAFE_CAST(CONCAT(SUBSTR(SAFE_CAST(EXTRACT(date FROM read_time) as STRING),1,8),'01') as DATE) as month,
-- (CASE WHEN articles.article = 1 THEN "article"
-- WHEN articles.video_play = 1 THEN "video_plays" END) as content_type,
SUM(articles.video_play) as video_plays,
COUNT(DISTINCT articles.unique_id) as uniques,
COUNT(DISTINCT articles.views) as views,
COUNT(DISTINCT CONCAT(articles.unique_id, articles.visit_num)) as visits,
COUNT(DISTINCT IF(articles.visit_num = sales.visit_num, post_purchaseid, null)) as orders_same_visit,
COUNT(DISTINCT post_purchaseid) as orders_7_days,
COUNT(DISTINCT post_purchaseid) as purchases,
COUNT(DISTINCT articles.unique_id) as unique_ids,
COUNT(DISTINCT post_purchaseid) / COUNT(DISTINCT articles.unique_id) as conversion_rate
FROM
articles
LEFT JOIN
sales ON
articles.unique_id = sales.unique_id
AND articles.read_time < sales.subscribe_time
AND DATE_DIFF(DATE(sales.subscribe_time), DATE(articles.read_time), DAY) < 7

GROUP BY
referrer,
month
having uniques > 500
#and content_type IS NOT NULL

ORDER BY
uniques DESC)










UNION ALL










(WITH articles AS (SELECT
DISTINCT
visit_referrer,
#timestamp(date_time) as read_time,
safe_cast(date_time as timestamp) as read_time,
IF(REGEXP_CONTAINS(page_url, 'utm_medium=news_tab') AND NET.REG_DOMAIN(visit_referrer) = 'facebook.com', 1, 0) as news_tab,
IF(REGEXP_CONTAINS(post_evar56, 'personalized'), 1, 0) as locked,
CASE WHEN post_page_event = '0' AND (CAST(exclude_hit AS INT64) <= 0 AND hit_source NOT IN ('5', '8', '9')) AND prop1 = "Article" THEN concat(post_visid_high, post_visid_low, visit_num, visit_page_num) ELSE NULL END as views,
IF(post_prop27 IN ('Marketwatch_sub_yes','MW_sub_yes'), 1, 0) as sub_status,
(CASE WHEN prop1 = "Article" THEN CONCAT(post_visid_high, post_visid_low) END) as unique_id,
visit_num,
#IF(prop1 = 'Article', 1, 0) as article,
IF(post_page_event = "50",1,0) as video_play,
visit_page_num,
#IF(REGEXP_CONTAINS(visit_start_pagename, 'Article'), 1, 0) as article_visit,
#IF(post_video <> " ", 1, 0) as video_visit,
IF(page_event_var2 = 'MW_Article_social_share', 1, 0) as share,
prop10
FROM `djomniture.cipomniture_djglobal.*`
WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^\d{4}$*')
AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -14 MONTH), MONTH)
-- AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
#MARKETWATCH SEGMENT
and (LOWER(channel) LIKE '%marketwatch%'                                                                
OR LOWER(channel) LIKE '%bigchart%'                                                                
OR ((post_pagename = 'https://m-secure.wsj.net/amp/omniture.html' OR post_pagename like 'https://mw1.wsj.net/amp/public/fbia/omniture.html')                                                                
AND (LOWER(post_prop11) LIKE '%facebookinstant%'                                                                
OR LOWER(post_prop11) LIKE '%googleamp%'))                                                                
OR (LOWER(post_pagename) LIKE '%bigcharts%'                                                                
OR LOWER(post_pagename) LIKE '%.marketwatch.com/%')                                                                
OR LOWER(post_pagename) LIKE 'mwbc\\_%'                                                                
OR LOWER(post_pagename) LIKE 'bc\\_%'                                                                
OR LOWER(user_server) = 'bigcharts.marketwatch.com'                                                                
OR ((post_prop1 IS NULL                                                                
OR post_prop1 = '')                                                                
AND LOWER(post_prop24) = 'edition_')OR lower(page_url) like "%www.marketwatch.com%")


#and prop1 = "Article"
),
--AND (REGEXP_CONTAINS(page_url, 'wsj[.com|.net]')
--OR channel IN ('Online Journal'))),
sales as (SELECT
CONCAT(post_visid_high, post_visid_low) as unique_id,
timestamp(date_time) as subscribe_time,
post_evar39 as conversion_headline,
visit_num,
post_purchaseid,
FROM
`djomniture.cipomniture_djglobal.*`
WHERE REGEXP_CONTAINS(_TABLE_SUFFIX,r'^\d{4}$*')
AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -14 MONTH), MONTH)
-- AND DATE(TIMESTAMP(CONCAT(SUBSTR(_TABLE_SUFFIX,1,4),'-',SUBSTR(_TABLE_SUFFIX,-2),'-','01'))) BETWEEN DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -3 MONTH), MONTH) AND DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY)
AND SAFE_CAST(exclude_hit AS INT64) <= 0
AND hit_source NOT IN ('5',
'8',
'9')
AND REGEXP_CONTAINS(post_event_list, r'^1,|,1,|,1$')
--Relevant order filters go here (i.e. excluding student)
AND REGEXP_CONTAINS(page_url, 'store.marketwatch.com|buy.marketwatch.com')
AND channel IN ('MarketWatch','Marketwatch'))

SELECT
CASE
      #WHEN REGEXP_CONTAINS(page_url, 'utm_medium=news_tab') AND NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook News'
      WHEN news_tab = 1 THEN "Facebook News Tab"
      WHEN prop10 IN ('MW_mw_RHF','_mw_RHF')#,'WSJ_wsj_RHF','BOL_bar_RHF')
        OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'robinhood') THEN 'Robinhood'
      WHEN REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'goog|android.gm') THEN 'Google'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'ampproject.org' THEN 'Google'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'investors.com' THEN 'IBD'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'newsbreakapp.com' THEN 'NewsBreak'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook Main Feed'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'mansionglobal.com' THEN 'Mansion Global'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'realclearpolitics.com' THEN 'RealClearPolitics'
      WHEN NET.REG_DOMAIN(visit_referrer) = 't.co'
        OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'twitter') THEN 'Twitter'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'linkedin.com' OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'lnkd') OR REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'linked') THEN 'LinkedIn'
      WHEN visit_referrer LIKE "%finance.yahoo%" THEN 'Yahoo Finance'
      WHEN REGEXP_CONTAINS(NET.REG_DOMAIN(visit_referrer), 'yahoo') THEN 'Yahoo'
      WHEN NET.REG_DOMAIN(visit_referrer) IS NULL OR NET.REG_DOMAIN(visit_referrer) = 'marketwatch.com' THEN 'Direct'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'apple.news' THEN 'Apple News'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'facebook.com' THEN 'Facebook'
      #WHEN NET.REG_DOMAIN(visit_referrer) = 'News Tab' THEN 'FB News Tab'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'smartnews.com' THEN 'SmartNews'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'snapchat.com' THEN 'Snapchat'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'apple.com' THEN 'Apple Stocks'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'marketwatch.com' THEN 'MarketWatch'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'wsj.com' THEN 'Wall Street Journal'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'barrons.com' THEN "Barron's"
      WHEN NET.REG_DOMAIN(visit_referrer) = 'bing.com' THEN 'Bing'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'msn.com' THEN 'MSN'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'reddit.com' THEN 'Reddit'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'instagram.com' THEN 'Instagram'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'flipboard.com' THEN 'Flipboard'
      WHEN NET.REG_DOMAIN(visit_referrer) = 'youtube.com' THEN 'YouTube'
      WHEN prop10 LIKE '%cashapprss%' THEN 'Cash App'
      WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%drudge%' THEN 'Drudge Report'
      #WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%wsj_taboola_rss%' THEN 'Taboola News'
      WHEN prop10 LIKE '%mktwch_taboola_rss%' THEN 'Taboola News'
      #WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%bar_taboola_rss%' THEN 'Taboola News'
       #this can probably be shortened to %taboola_rss%
      #WHEN NET.REG_DOMAIN(visit_referrer) LIKE '%WSJ_Euronews%' THEN 'Euronews'
      WHEN prop10 LIKE '%mw_square%' THEN "Cash App"
      WHEN NET.REG_DOMAIN(articles.visit_referrer) IS NULL THEN 'marketwatch.com'
    ELSE
    NET.REG_DOMAIN(visit_referrer) END as referrer,
"MarketWatch" as brand,
SAFE_CAST(CONCAT(SUBSTR(SAFE_CAST(EXTRACT(date FROM read_time) as STRING),1,8),'01') as DATE) as month,
-- (CASE WHEN articles.article = 1 THEN "article"
-- WHEN articles.video_play = 1 THEN "video_plays" END) as content_type,
SUM(articles.video_play) as video_plays,
COUNT(DISTINCT articles.unique_id) as uniques,
COUNT(DISTINCT articles.views) as views,
COUNT(DISTINCT CONCAT(articles.unique_id, articles.visit_num)) as visits,
COUNT(DISTINCT IF(articles.visit_num = sales.visit_num, post_purchaseid, null)) as orders_same_visit,
COUNT(DISTINCT post_purchaseid) as orders_7_days,
COUNT(DISTINCT post_purchaseid) as purchases,
COUNT(DISTINCT articles.unique_id) as unique_ids,
COUNT(DISTINCT post_purchaseid) / COUNT(DISTINCT articles.unique_id) as conversion_rate
FROM
articles
LEFT JOIN
sales ON
articles.unique_id = sales.unique_id
AND articles.read_time < sales.subscribe_time
AND DATE_DIFF(DATE(sales.subscribe_time), DATE(articles.read_time), DAY) < 7

GROUP BY
referrer,
month
having uniques > 1000
#and content_type IS NOT NULL

ORDER BY
uniques DESC)