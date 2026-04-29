USE ROLE accountadmin;
USE WAREHOUSE compute_wh;

CREATE OR REPLACE DATABASE fx_handson;
CREATE OR REPLACE SCHEMA fx_handson_schema;

USE DATABASE fx_handson;
USE SCHEMA fx_handson_schema;
CREATE OR REPLACE STAGE fx_handson_schema.udf_stage;

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION'; 

-- Git連携のため、API統合を作成する
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-kmotokubota/')
  ENABLED = TRUE;

-- GIT統合の作成(今は仮)
CREATE OR REPLACE GIT REPOSITORY GIT_INTEGRATION_FOR_HANDSON
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/sfc-gh-kmotokubota/fx_handson.git';

-- Notebookの作成
CREATE OR REPLACE NOTEBOOK fx_handson
    FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/fx_handson
    MAIN_FILE = 'fx_handson.ipynb'
    QUERY_WAREHOUSE = COMPUTE_WH
    WAREHOUSE = COMPUTE_WH;

-- Streamlit in Snowflakeの作成
CREATE OR REPLACE STREAMLIT fx_analysis_app
    FROM @GIT_INTEGRATION_FOR_HANDSON/branches/main/fx_handson
    MAIN_FILE = 'fx_handson_app.py'
    QUERY_WAREHOUSE = COMPUTE_WH;


