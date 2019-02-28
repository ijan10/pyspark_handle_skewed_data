For using this code please be notice the bellow 2 methods:
left_join_with_skew_key
repartition_dfs_for_join

The first one is in case you wish to do simple join without additional expression and the function return transformation datafarme of the join results.
The second one is for cases you wish to add an expression or customize the join. In such case the method returns the dataframe after repartition with bin_id. Please keep in mind bin_id is now part of the join. We tested this solution with "big" and "small" tables with broadcast hint.
In my scenario, we succeed to reduce running time of the job from 6 hours to total 1-hour chain of jobs (on the same type of cluster). Also, the job became much more stable.