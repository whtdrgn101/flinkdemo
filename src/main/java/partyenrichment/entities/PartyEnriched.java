package partyenrichment.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PartyEnriched extends PartyBase {
    private double PartyValueScore;
    private double PartyPriorityScore;
}
