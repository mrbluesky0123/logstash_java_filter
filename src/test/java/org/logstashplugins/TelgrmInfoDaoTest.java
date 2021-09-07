package org.logstashplugins;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

public class TelgrmInfoDaoTest {

    @Test
    public void getTelgrmInfoTest() {
        TelgramInfoDao telgramInfoDao = new TelgramInfoDao();
        List<TelgrmInfo> telgrmInfos = telgramInfoDao.getTelgrmInfo("A400");
        Object f = "10:52:40:60 R __ __ 000000 0827000002 A400 A4004810202108271052090827000002ON0556    31441601001A              K20091757       2208160348202108271052092172010000038088=0000                                                                                                                                                                                                                                                                                                                                                                                                 1140N0000063150080000063150                                              ";
        Iterator<TelgrmInfo> it = telgrmInfos.iterator();
        String telgrmString = ((String) f).substring(43);
        int telgrmLength = telgrmString.length();
        int nextIndex = 0;
        while(it.hasNext()) {
            TelgrmInfo telgrmInfo = it.next();
            String stringToFill = "";
            if(nextIndex < telgrmLength) {
                stringToFill = telgrmString.substring(nextIndex, nextIndex + telgrmInfo.getFieldSize());
            }
            System.out.println(telgrmInfo.getField() + "(" + telgrmInfo.getFieldSize() + "): " + stringToFill);
            nextIndex += telgrmInfo.getFieldSize();
        }

        for(TelgrmInfo telgrmInfo: telgrmInfos) {
            System.out.println(telgrmInfo);
        }
        Assert.assertEquals("organ_cd", telgrmInfos.get(1).getField());
    }

}
