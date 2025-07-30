@echo off
>C:\Logs\load_currency_cmd.txt 2>&1(
  cd c:\Services\prod\LoadPrices\
  call .\.venv\Scripts\activate
  python load_currency.py
) 


@REM cd c:\Services\prod\LoadPrices\
@REM call .\.venv\Scripts\activate
@REM python load_currency.py
