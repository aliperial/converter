cat twitter_new.dump | grep  "^create (" | grep ")$" > entities.dump
cat twitter_new.dump | grep  "^create _" | grep "\`]->_" | grep "CONTAINS\|MENTIONS\|POSTS\|REPLY_TO\|RETWEETS\|TAGS\|USING" > relations.dump
cat relations.dump | sed -rn 's/create _([[:digit:]]+)-\[:`([^`]+)`\]->_([[:digit:]]+)/INSERT INTO R_\2 (s_id,d_id) values (\1, \3); /p' | sort  > relations.sql
cat relations.dump | sed -rn 's/create _([[:digit:]]+)-\[:`([^`]+)`\]->_([[:digit:]]+)/CREATE TABLE R_\2(s_id bigint references e_XXXX (_id), d_id bigint references e_YYYY (_id) );/p' | sort | uniq > relations_create.sql

