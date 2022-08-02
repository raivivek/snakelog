# Snakelog

Script that parses your [Snakemake](https://snakemake.readthedocs.io) job logs to provide an easier way to check job status and current progress.

# Installation

```
pip install git+https://github.com/raivivek/snakelog
```

# Examples

## `status`
```
> snakelog status
Summary of log file: .snakemake/log/2023-08-23T230733.352042.snakemake.log

rule                  number_of_jobs    queued    failed    finished
------------------  ----------------  --------  --------  ----------
all                                1         0         0           0
consensus_all                      1         0         1           0
overlap_with_peaks               400         0         0         400

Only finished 400 (99%) and failed 1 (0%) of total 402 planned jobs.

## `logs`

❯ snakelog logs
1       .snakemake/log/2023-08-23T230733.352042.snakemake.log
2       .snakemake/log/2023-08-23T181823.838448.snakemake.log
3       .snakemake/log/2023-08-23T164939.199498.snakemake.log
4       .snakemake/log/2023-08-23T161840.909772.snakemake.log
5       .snakemake/log/2023-08-23T154451.411233.snakemake.log
6       .snakemake/log/2023-08-23T125441.906520.snakemake.log
7       .snakemake/log/2023-08-23T121424.946548.snakemake.log
8       .snakemake/log/2023-08-18T115941.083347.snakemake.log
9       .snakemake/log/2023-08-17T141927.342097.snakemake.log
10      .snakemake/log/2023-08-17T140958.717616.snakemake.log
... 31 more logs
```

## `history`

```
❯ snakelog history
timestamp           |uuid      |num_jobs  |queued    |failed    |finished
---------           |----      |--------  |------    |------    |--------
11:07:33 PM, Aug 23 |352042    |401       |0         |1         |400
06:18:23 PM, Aug 23 |838448    |205       |0         |1         |204
04:49:39 PM, Aug 23 |199498    |669       |0         |1         |668
```

# Usage

```
NAME
    snakelog --path .

SYNOPSIS
    snakelog --path . COMMAND | VALUE

COMMANDS
    COMMAND is one of the following:

     dryrun
       Dry-run Snakemake

     history
       View Snakemake execution history with job stats

     jobdetails
       Print detailed job information for jobid in n-th most recent execution

     jobs
       Print job summary for n-th most recent execution; detailed status

     lockstatus
       Print the lock status of current directory

     log
       Print the path of most recent n-th log file

     logs
       Print the path of last n logs

     show
       Show a log file contents - alias for view

     status
       Get log path and print summary of log file

     unlock
       Unlock the Snakemake directory

     view
       Show a log file contents

VALUES
    VALUE is one of the following:

     log_dir

     path
```

# License

See [LICENSE](LICENSE).