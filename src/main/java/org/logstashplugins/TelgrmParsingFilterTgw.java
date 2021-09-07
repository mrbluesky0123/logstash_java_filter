package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.Filter;
import co.elastic.logstash.api.FilterMatchListener;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

// class name must match plugin name
@LogstashPlugin(name = "telgrm_parsing_filter_tgw")
public class TelgrmParsingFilterTgw implements Filter {

    public static final PluginConfigSpec<String> SOURCE_CONFIG =
            PluginConfigSpec.stringSetting("source", "message");

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
                List<TelgrmInfo> telgrmInfos = this.telgramInfoDao.getTelgrmInfo(telgrmNo);

                Iterator<TelgrmInfo> it = telgrmInfos.iterator();
                int telgrmLength = telgrmString.length();
                int nextIndex = 0;
                while(it.hasNext()) {
                    TelgrmInfo telgrmInfo = it.next();
                    String stringToFill = "";
                    if(nextIndex < telgrmLength) {
                        stringToFill = telgrmString.substring(nextIndex, nextIndex + telgrmInfo.getFieldSize());
                    }
                    e.setField(telgrmInfo.getField(), stringToFill);
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
