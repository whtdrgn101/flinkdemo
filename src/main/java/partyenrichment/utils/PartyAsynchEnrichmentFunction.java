package partyenrichment.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import partyenrichment.entities.PartyBase;
import partyenrichment.entities.PartyEnriched;

import java.util.Collections;

public class PartyAsynchEnrichmentFunction extends RichAsyncFunction<PartyBase, PartyEnriched> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void asyncInvoke(final PartyBase s, final ResultFuture<PartyEnriched> resultFuture) throws Exception {
        PartyEnriched party = new PartyEnriched();
        party.setPartyId(s.getPartyId());
        party.setPartyName(s.getPartyName());
        party.setPartyPhone(s.getPartyPhone());
        party.setPartyPriorityScore(0.3);
        party.setPartyValueScore(10.0);

        resultFuture.complete(Collections.singletonList(party));
    }
}
