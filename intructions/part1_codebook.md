# 2024-precincts
Repository for cleaning precinct data from the general election on November 5, 2024

## Fields:

### precinct: 
The string id for the smallest election reporting unit of a state. Should generally be left exactly the way it is (this includes not capitalizing it), except under two conditions. 1) if the precinct is actually some type of total or aggregation of precinct results, then it should be dropped. 2) If we already know what the precinct id looks like from some type of precinct shapefile from a state, then the precinct id should be structured to match the shapefile precinct id. 

### office: 
The field which contains the name of the elected position for the race. These should be standardized and stripped of the district code, candidate names, parties, etc. that belong in the other fields. All entries should be in upper case. Standard entries are US PRESIDENT, US SENATE, US HOUSE, GOVERNOR, STATE SENATE, and STATE HOUSE. When a rows holds meta-information like the number of registered voters in a jurisdiction, the label is stored in the office column, and the candidate column is left blank.

### party_detailed:
The upper case party name for the given entry. The most common entries will be DEMOCRAT, REPUBLICAN, and LIBERTARIAN, with the full detailed names for the various parties, including those names that are unique to a given state (i.e. party fusion names).

Special Cases
- Propositions, amendment, and other referenda should have a blank value for this field
- Undervotes and Overvotes should have a blank value for this field

### party_simplified:
The upper case party name for the given entry. The entries will be one of: DEMOCRAT, REPUBLICAN, LIBERTARIAN, OTHER, NONPARTISAN, and <NA>. Propositions, amendment, and other referenda should have a blank value for this field.

### votes:
The numeric value of votes for a given entry. Ensure that commas and the like are not included so as to ensure that it is numeric and not string, and any missing values should be coded as 0. 

Special cases:
- If any votes have been redacted (most common in small precincts in certain states), please code it as the asterisk character `*`.

### county_name:
The upper case name of the county. 

### county_fips: 
The Census 5-digit code for a given county. Structured such that the first two digits are the state fips, and the last three digits are the county part of the fips. Ensure that each component is string padded such that if a state's or county's fip is one digit, i.e. AL, then padded such that it might take the form of 01020. 

### jurisdiction_name:
The upper case name for the jurisdiction. With the exception of New England states, Wisconsin, and Alaska, these will be the same as the county_name. For the New England states, these will be the town names. 

### candidate:
The candidate name. Should be all upper case and punctuation. We standardize candidate names within states. Across states we only need to standardize candidate names for US PRESIDENT. We have three other main standardization conventions. Write overvotes as `OVERVOTES`, undervotes as `UNDERVOTES`, and (wherever total number of write-in votes are given rather than individual write-in candidates' totals) denote write in totals as `WRITE-IN`. For US PRESIDENT, in 2024, here are some of the candidate names: DONALD J TRUMP, KAMALA D HARRIS, CHASE OLIVER, CLAUDIA DE LA CRUZ, JILL STEIN, RANDALL TERRY, PETER SONSKI, ROBERT F KENNEDY, CORNEL WEST, JOSEPH KISHORE, RACHELE FRUIT. Otherwise, please just standardize the candidate to only the presidential candidate's name (ie, not the vice president).  

### year:
The year of the election.

### stage:
The stage of the election, can be "PRI" for primary, "GEN" for general, or "RUNOFF" for a runoff election. 

### state: 
The name of the state in capitals. 

### special:
An indicator for whether the election was a special election, "TRUE" if special, "FALSE" for non-special. 

### writein:
An indicator for whether the candidate was a write in, "TRUE" if write in, "FALSE" otherwise. Note that entries noted as "scattering" are write in votes, and should be noted as TRUE. 

### date: 
The date of the primary/election. Note that there will be some states with different election dates for different offices (i.e. presidential primary v. congressional primary). Should be formatted as %y-%m-%d, such that January 5, 2019 would be "2019-01-05" 

