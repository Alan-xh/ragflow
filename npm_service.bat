@echo off
echo start npm run dev...
pushd "%~dp0\web"
npm run dev
popd

echo Finish
pause