@echo off
>C:\Logs\load_currency_cmd.txt 2>&1(

  cd c:\scripts\

  call .\.venv\Scripts\activate

  python load_currency.py

) 



