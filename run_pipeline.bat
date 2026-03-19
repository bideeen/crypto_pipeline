@echo off
echo Starting Crypto Pipeline - %date% %time%

cd /d C:\Users\Abideen Bello\dbt_projects\crypto_pipeline

call crypto\Scripts\activate.bat

python run_pipeline.py >> logs\pipeline.log 2>&1

echo Pipeline finished - %date% %time%