* Get Data

```mermaid
graph TB
Start --> A[Get Symbol & Trans Date List]
A --> B{Check If latest date is None}
B --N--> C{Check If latest date is later than today}
B --Y--> D[Insert the past year data]
C --N--> E[Insert new data]
C --Y--> End(Do Nothing)
```