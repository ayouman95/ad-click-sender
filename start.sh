num=`ps axfu | grep ad_click_sender | grep -v grep | wc -l`
if [ $num -ge 1 ]; then
    echo "ad_click_server or supervisor has started"
    exit 0
fi
nohup ./ad_click_sender >> ./nohup.log 2>&1 &
