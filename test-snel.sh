#!/bin/bash
sqlite3-grandata <<EOF
.load libsnel_sqlited
CREATE VIRTUAL TABLE table1 USING SNEL('table1.snel');

SELECT SNEL_SET_OUTPUT(0);
SELECT COUNT(*) FROM table1 WHERE int32col IS NOT NULL AND int64col > 100;
SELECT SNEL_SET_OUTPUT(1);
--SELECT COUNT(*) FROM table1 WHERE int32col IS NOT NULL AND int64col > 100;

SELECT int32col, SUM(int64col) FROM table1 WHERE int32col IS NOT NULL AND int64col > 100 GROUP BY int32col;
EOF