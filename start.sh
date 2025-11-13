num=`ps axfu | grep adx_server | grep -v grep | wc -l`
if [ $num -ge 1 ]; then
    echo "ad_click_server or supervisor has started"
    exit 0
fi
nohup ./ad_click_sender start >> ./nohup.log 2>&1 &
