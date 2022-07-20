package partyenrichment.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PartyBase {
    private long PartyId;
    private String PartyName;
    private String PartyPhone;
}