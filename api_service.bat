@echo off
echo start ragflow_server.py...
pushd "%~dp0\api"
python ragflow_server.py
popd

echo start npm run dev...
pushd "%~dp0\app"
npm run dev
popd

echo Finish
pause