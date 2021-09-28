package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.Filter;
import co.elastic.logstash.api.FilterMatchListener;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.util.*;

// class name must match plugin name
@LogstashPlugin(name = "telgrm_parsing_filter_tgw")
public class TelgrmParsingFilterTgw implements Filter {

    public static final PluginConfigSpec<String> SOURCE_CONFIG =
            PluginConfigSpec.stringSetting("source", "message");
    public static Map<String, List<TelgrmInfo>> telgrmMap = new HashMap<>();
    private String id;
    private String sourceField;
    private TelgramInfoDao telgramInfoDao;

    public TelgrmParsingFilterTgw(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.sourceField = config.get(SOURCE_CONFIG);
        this.telgramInfoDao = new TelgramInfoDao();
    }

    @Override
    public Collection<Event> filter(Collection<Event> events, FilterMatchListener matchListener) {
        for (Event e : events) {
            Object f = e.getField(sourceField);
            if (f instanceof String) {
//                e.setField(sourceField, StringUtils.reverse((String)f));
                String telgrmNo = ((String) f).substring(38, 42);
                String telgrmString = ((String) f).substring(43);
                byte [] telgrmByte = new byte[0];
                try {
                    telgrmByte = telgrmString.getBytes("euc-kr");
                } catch (UnsupportedEncodingException unsupportedEncodingException) {
                    unsupportedEncodingException.printStackTrace();
                }

                if(telgrmMap == null) {
                    telgramInfoDao.getTelgrmInfo();
                }

                List<TelgrmInfo> telgrmInfos = telgrmMap.get(telgrmNo);
                if(telgrmInfos == null || telgrmInfos.size() == 0) {
                    return events;
                }
                Iterator<TelgrmInfo> it = telgrmInfos.iterator();
                int telgrmLength = telgrmString.length();
                int nextIndex = 0;
                while(it.hasNext()) {
                    TelgrmInfo telgrmInfo = it.next();
                    int begin = nextIndex;
                    if(nextIndex >= telgrmLength) {
                        break;
                    }
                    int end = nextIndex + telgrmInfo.getFieldSize();
                    if(end >= telgrmLength) {
                        end = telgrmLength;
                    }

                    String nextField = "";
                    byte[] nextFieldByte = Arrays.copyOfRange(telgrmByte, begin, end);
                    try {
                        nextField = new String(nextFieldByte, "euc-kr");
                    } catch (UnsupportedEncodingException unsupportedEncodingException) {
                        unsupportedEncodingException.printStackTrace();
                    }

                    e.setField(telgrmInfo.getField(), nextField);
                    nextIndex += telgrmInfo.getFieldSize();
                }

                matchListener.filterMatched(e);
            }
        }
        return events;
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Collections.singletonList(SOURCE_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
