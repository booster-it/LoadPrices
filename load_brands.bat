rem
@echo off
>C:\Logs\load_brands_cmd.txt 2>&1(

  cd c:\Services\prod\LoadPrices\

  call .\.venv\Scripts\activate

  python load_brands.py

) 
