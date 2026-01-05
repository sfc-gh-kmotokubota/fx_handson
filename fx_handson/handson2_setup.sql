-- ===================================================================================
-- FXハンズオン セットアップSQL（改訂版）
-- AI_COMPLETEではなく、直接CTASでセマンティックビューを作成
-- ===================================================================================

USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

USE DATABASE fx_handson;
USE SCHEMA fx_handson_schema;

-- ===================================================================================
-- STEP 1: セマンティックビュー作成用の為替データテーブルを作成
-- ===================================================================================
CREATE OR REPLACE TABLE FX_ALL_ANALYSIS AS
SELECT
    DATE,
    VARIABLE_NAME,
    BASE_CURRENCY_ID,
    QUOTE_CURRENCY_ID,
    VALUE AS EXCHANGE_RATE,
    
    -- 移動平均の計算
    AVG(VALUE) OVER (
        PARTITION BY BASE_CURRENCY_ID, QUOTE_CURRENCY_ID
        ORDER BY DATE 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS MA_5,
    
    AVG(VALUE) OVER (
        PARTITION BY BASE_CURRENCY_ID, QUOTE_CURRENCY_ID
        ORDER BY DATE 
        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
    ) AS MA_20,
    
    AVG(VALUE) OVER (
        PARTITION BY BASE_CURRENCY_ID, QUOTE_CURRENCY_ID
        ORDER BY DATE 
        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
    ) AS MA_50,
    
    -- 日次リターン（前日比変化率）
    (VALUE - LAG(VALUE, 1) OVER (
        PARTITION BY BASE_CURRENCY_ID, QUOTE_CURRENCY_ID 
        ORDER BY DATE
    )) / NULLIF(LAG(VALUE, 1) OVER (
        PARTITION BY BASE_CURRENCY_ID, QUOTE_CURRENCY_ID 
        ORDER BY DATE
    ), 0) * 100 AS DAILY_RETURN,
    
    -- 前日比変化額
    VALUE - LAG(VALUE, 1) OVER (
        PARTITION BY BASE_CURRENCY_ID, QUOTE_CURRENCY_ID 
        ORDER BY DATE
    ) AS DAILY_CHANGE
    
FROM SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.FX_RATES_TIMESERIES
ORDER BY DATE DESC;

-- ===================================================================================
-- STEP 2: セマンティックビューを直接作成（CTASスタイル）
-- ===================================================================================
CREATE OR REPLACE SEMANTIC VIEW FX_ALL_SEMANTIC_VIEW
    TABLES (
        FX_ALL_ANALYSIS AS FX_HANDSON.FX_HANDSON_SCHEMA.FX_ALL_ANALYSIS
            WITH SYNONYMS = ('為替レート', '外国為替', 'FXデータ', '通貨レート', '為替相場')
            COMMENT = '為替レートの時系列データと移動平均、日次リターンを含む分析用テーブル'
    )
    FACTS (
        -- 基本的な数値データ
        FX_ALL_ANALYSIS.exchange_rate AS EXCHANGE_RATE
            WITH SYNONYMS = ('為替レート', 'レート', '交換レート')
            COMMENT = '基軸通貨と決済通貨間の為替レート',
        FX_ALL_ANALYSIS.ma_5_value AS MA_5
            WITH SYNONYMS = ('5日移動平均', '5日MA', '短期移動平均')
            COMMENT = '5日間の移動平均値',
        FX_ALL_ANALYSIS.ma_20_value AS MA_20
            WITH SYNONYMS = ('20日移動平均', '20日MA', '中期移動平均')
            COMMENT = '20日間の移動平均値',
        FX_ALL_ANALYSIS.ma_50_value AS MA_50
            WITH SYNONYMS = ('50日移動平均', '50日MA', '長期移動平均')
            COMMENT = '50日間の移動平均値',
        FX_ALL_ANALYSIS.daily_return_pct AS DAILY_RETURN
            WITH SYNONYMS = ('日次リターン', '日次変化率', '前日比率')
            COMMENT = '前日比の変化率（パーセント）',
        FX_ALL_ANALYSIS.daily_change_value AS DAILY_CHANGE
            WITH SYNONYMS = ('日次変化額', '前日比変化', '変動幅')
            COMMENT = '前日比の変化額',
        -- 計算用ファクト
        FX_ALL_ANALYSIS.record_count AS 1
            COMMENT = 'レコード数カウント用'
    )
    DIMENSIONS (
        -- 日付関連
        FX_ALL_ANALYSIS.trade_date AS DATE
            WITH SYNONYMS = ('日付', '取引日', '営業日')
            COMMENT = '為替レートの基準日',
        FX_ALL_ANALYSIS.trade_year AS YEAR(DATE)
            WITH SYNONYMS = ('年', '取引年')
            COMMENT = '取引年',
        FX_ALL_ANALYSIS.trade_month AS MONTH(DATE)
            WITH SYNONYMS = ('月', '取引月')
            COMMENT = '取引月',
        FX_ALL_ANALYSIS.trade_quarter AS QUARTER(DATE)
            WITH SYNONYMS = ('四半期', 'クォーター')
            COMMENT = '取引四半期',
        FX_ALL_ANALYSIS.day_of_week AS DAYOFWEEK(DATE)
            WITH SYNONYMS = ('曜日', '週の日')
            COMMENT = '曜日（1=日曜日〜7=土曜日）',
        -- 通貨関連
        FX_ALL_ANALYSIS.variable_name AS VARIABLE_NAME
            WITH SYNONYMS = ('変数名', '通貨ペア名', 'ペア名')
            COMMENT = '為替ペアの変数名',
        FX_ALL_ANALYSIS.base_currency AS BASE_CURRENCY_ID
            WITH SYNONYMS = ('基軸通貨', 'ベース通貨', '元通貨')
            COMMENT = '基軸通貨コード（例：USD, EUR, JPY）',
        FX_ALL_ANALYSIS.quote_currency AS QUOTE_CURRENCY_ID
            WITH SYNONYMS = ('決済通貨', 'クォート通貨', '対象通貨')
            COMMENT = '決済通貨コード（例：JPY, USD, EUR）'
    )
    METRICS (
        FX_ALL_ANALYSIS.total_records AS COUNT(FX_ALL_ANALYSIS.record_count)
            WITH SYNONYMS = ('総レコード数', '件数', 'データ件数')
            COMMENT = 'データの総レコード数',
        FX_ALL_ANALYSIS.avg_exchange_rate AS AVG(FX_ALL_ANALYSIS.exchange_rate)
            WITH SYNONYMS = ('平均為替レート', '平均レート')
            COMMENT = '為替レートの平均値',
        FX_ALL_ANALYSIS.max_exchange_rate AS MAX(FX_ALL_ANALYSIS.exchange_rate)
            WITH SYNONYMS = ('最高為替レート', '最高値', '高値')
            COMMENT = '為替レートの最大値',
        FX_ALL_ANALYSIS.min_exchange_rate AS MIN(FX_ALL_ANALYSIS.exchange_rate)
            WITH SYNONYMS = ('最低為替レート', '最安値', '安値')
            COMMENT = '為替レートの最小値',
        FX_ALL_ANALYSIS.avg_daily_return AS AVG(FX_ALL_ANALYSIS.daily_return_pct)
            WITH SYNONYMS = ('平均日次リターン', '平均変化率')
            COMMENT = '日次リターンの平均値',
        FX_ALL_ANALYSIS.total_volatility AS STDDEV(FX_ALL_ANALYSIS.daily_return_pct)
            WITH SYNONYMS = ('ボラティリティ', '変動率', '標準偏差')
            COMMENT = '日次リターンの標準偏差（価格変動の大きさ）',
        FX_ALL_ANALYSIS.price_range AS MAX(FX_ALL_ANALYSIS.exchange_rate) - MIN(FX_ALL_ANALYSIS.exchange_rate)
            WITH SYNONYMS = ('価格レンジ', '変動幅', '高値安値差')
            COMMENT = '最高値と最安値の差'
    )
    COMMENT = '為替レートの時系列分析用セマンティックビュー。移動平均、日次リターン、ボラティリティなどの分析が可能';

-- ===================================================================================
-- STEP 3: Cortex Agentの作成
-- ===================================================================================
CREATE OR REPLACE AGENT FX_HANDSON.FX_HANDSON_SCHEMA.FX_ANALYST_AGENT
WITH PROFILE = '{ "display_name": "fx_analyst_agent" }'
    COMMENT = $$ 為替レートに関する質問に回答するエージェントです。 $$
FROM SPECIFICATION $$
{
  "models": {
    "orchestration": ""
  },
  "instructions": {
    "response": "あなたは為替レート（FX）データにアクセスできるデータアナリストです。ユーザーが日付範囲を指定しない場合は、直近1年間のデータを使用してください。すべてのデータを活用して、ユーザーの質問を分析し回答してください。可能であれば視覚化を提供してください。トレンドラインは線グラフをデフォルトとし、カテゴリ比較は棒グラフを使用してください。",
    "orchestration": "為替レートの分析に関する質問にはCortex Analystを使用してください。通貨ペア、日付範囲、移動平均、ボラティリティなどの条件を適切に解釈してクエリを実行してください。",
    "sample_questions": [
      {
        "question": "USD/JPYの過去1ヶ月間の為替レートの推移を折れ線グラフで表示してください"
      },
      {
        "question": "主要通貨ペアの平均日次リターンとボラティリティを比較してください"
      },
      {
        "question": "EUR/JPYの5日移動平均と20日移動平均のゴールデンクロス・デッドクロスを特定してください"
      },
      {
        "question": "今週最もボラティリティが高かった通貨ペアはどれですか"
      }
    ]
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "FX RATE DATAMART",
        "description": "為替レートデータをクエリするためのツールです。為替レート、移動平均、日次リターン、ボラティリティなどの分析が可能です。"
      }
    }
  ],
  "tool_resources": {
    "FX RATE DATAMART": {
      "semantic_view": "FX_HANDSON.FX_HANDSON_SCHEMA.FX_ALL_SEMANTIC_VIEW"
    }
  }
}
$$;

-- ===================================================================================
-- 確認クエリ
-- ===================================================================================
-- セマンティックビューの確認
SHOW SEMANTIC VIEWS IN SCHEMA FX_HANDSON.FX_HANDSON_SCHEMA;

-- エージェントの確認
SHOW AGENTS IN SCHEMA FX_HANDSON.FX_HANDSON_SCHEMA;

-- テーブルのサンプルデータ確認
SELECT * FROM FX_ALL_ANALYSIS LIMIT 10;
