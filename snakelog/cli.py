#! /usr/bin/env python3

"""
This module provides a command-line interface for interacting with Snakemake
workflows. It defines a `SnakeObject` class that represents a Snakemake
workflow and provides methods for viewing logs and lock status. It also defines
several utility functions for parsing Snakemake logs.
"""

import re
import sys
import glob
import pydoc
import logging
import itertools
import subprocess
from enum import Enum
from pathlib import Path
from shutil import which
from dateutil import parser
from dataclasses import dataclass
from collections import defaultdict

import fire
import tabulate
import snakemake.exceptions

logging.basicConfig(level=logging.INFO)

ERROR_LABELS = [
    "SyntaxError",
    "IndentationError",
    "OSError",
    "MissingInputException",
    "MissingOutputException",
    "AttributeError",
    "WorkflowError",
    "IncompleteFilesException",
    "AmbiguousRuleException",
    "KeyError",
    "NameError",
    "WildcardError",
    "Exception",
    "Error:",
    "[Errno",
    "KeyboardInterrupt",
    "Exiting because",
    "Terminating",
    *[x for x in snakemake.exceptions.__dir__() if x.endswith("Exception")],
]

SUCCESS_LABELS = ["(100%) done"]


class SnakemakeStatus(Enum):
    SUCCESS = 0
    ERROR = 1
    UNLOCKED = 2
    CATCHALL_ERROR = -1


def parse_rule_grammar(line, job_dict):
    # print(f"line>>>>{line}")
    if line.startswith("rule") or line.startswith("localrule"):
        return {"rule": line.strip(":").split()[1]}
    elif line.startswith("Error in"):
        return {"rule": line.strip(":").split()[-1], "status": "failed"}
    elif line.startswith("cluster_jobid"):
        return {"external_jobid": re.findall(r"\d+", line)[0]}
    elif line.startswith("Finished"):
        return {"status": "finished", "jobid": line.strip(".").split()[-1]}
    elif line.startswith("input"):
        return {"input": [x.strip() for x in line.split(":")[1].split(",")]}
    elif line.startswith("output"):
        return {"output": [x.strip() for x in line.split(":")[1].split(",")]}
    elif line.startswith("Error"):
        return {"status": "failed"}
    elif line.startswith("params"):
        return {
            "params": {
                k: v
                for k, v in [
                    x.strip().split("=") for x in line.split(":")[1].split(",")
                ]
            }
        }
    elif line.startswith("wildcards"):
        return {
            "wildcards": {
                k: v
                for k, v in [
                    x.strip().split("=") for x in line.split(":")[1].split(",")
                ]
            }
        }
    elif line.startswith("resources"):
        return {
            "resources": {
                k: v
                for k, v in [
                    x.strip().split("=") for x in line.split(":")[1].split(",")
                ]
            }
        }
    elif line.startswith("jobid"):
        return {"jobid": int(line.split(":")[1].strip())}
    elif line.startswith("log"):
        return {"log": line.split(":")[1].strip()}
    elif line.startswith("threads"):
        return {"threads": int(line.split(":")[1].strip())}
    elif line.startswith("Submitted"):
        external_jobid = re.findall(r"\d+", line)
        if external_jobid:
            external_jobid = external_jobid[1]
        return {"external_jobid": external_jobid, "status": "submitted"}
    elif any([line.startswith(x) for x in ERROR_LABELS]) and job_dict.get("status", None) != "finished":
        return {"status": "failed"}
    else:
        return {"kwargs": line.strip()}


def page_log(logfile):
    if which("bat") is not None:
        pydoc.pager = lambda x: pydoc.pipepager(x, "bat --style=plain --language log")
    return pydoc.pager(logfile)


@dataclass
class Job:
    timestamp: str = None
    rule: str = None
    input: list = None
    output: list = None
    jobid: int = None
    params: dict = None
    wildcards: dict = None
    resources: dict = None
    log: str = None
    threads: int = 1
    external_jobid: int = None
    external_jobname: str = None
    status: str = None
    kwargs: str = None

    def __str__(self):
        return f"{self.timestamp} {self.rule} {self.input} {self.output} {self.jobid} {self.params} {self.wildcards} {self.resources} {self.log} {self.threads} {self.external_jobid} {self.external_jobname} {self.kwargs}"

    def __repr__(self):
        return self.__str__()


@dataclass
class JobStats:
    name: str = None
    count: int = None
    min_threads: int = None
    max_threads: int = None

    def __str__(self):
        return f"{self.name} {self.count} {self.min_threads} {self.max_threads}"

    def __repr__(self):
        return self.__str__()


class SnakeObject(object):
    def __init__(self, path: str = "."):
        self.path = Path(path) / ".snakemake"
        self.log_dir = self.path / "log"

        if not self.path.exists():
            sys.stderr.write(
                f"{self.path} not found in current directory. Use --path to specify.\n"
            )
            sys.exit(1)

    @property
    def __logs(self):
        return sorted(
            glob.glob(str(self.log_dir / "*.log")),
            key=lambda x: parser.parse(Path(x).stem.replace(".snakemake", "")),
            reverse=True,
        )

    @property
    def _locked(self):
        if len(glob.glob(f"{self.path}/locks/*.lock")) > 0:
            return True
        return False

    def logs(self, n: int = 10):
        "Print the path of last n logs"
        for i, log in enumerate(self.__logs):
            if i > (n - 1):
                print(f"... {len(self.__logs) - n} more logs")
                break
            print(f"{i+1}\t{log}")

    def lockstatus(self):
        "Print the lock status of current directory"
        if self._locked:
            return sys.stdout.writelines(
                "Lock file exists --- Snakemake might be running!\n"
            )
        return sys.stdout.writelines("Snakemake is not running; no lock file found.\n")

    def dryrun(self, snakefile=None):
        "Dry-run Snakemake"
        try:
            sys.stderr.writelines(f"Dry-run mode\n------------\n")
            _snakefile = f"--snakefile {snakefile}" if snakefile else ""
            subprocess.run(f"snakemake -npr {_snakefile}", shell=True, check=True)
        except:
            sys.stderr.write("Error: Snakemake dry-run failed.\n")

    def unlock(self, snakefile=None):
        "Unlock the Snakemake directory"
        if self._locked:
            _snakefile = f"--snakefile {snakefile}" if snakefile else ""
            try:
                subprocess.run(
                    f"snakemake --unlock {_snakefile}", shell=True, check=True
                )
            except subprocess.CalledProcessError:
                return
        else:
            sys.stdout.writelines("No locks found.")

    def log(self, n: int = 1):
        "Print the path of most recent n-th log file"
        sys.stdout.write(f"{n}: {self.__logs[n-1]}\n")
        return

    def show(self, n: int = 1):
        "Show a log file contents - alias for view"
        with open(self.__logs[n - 1], "r") as f:
            page_log(f.read())

    def view(self, n: int = 0):
        "Show a log file contents"
        self.show(n)

    def _stats(self, log):
        stats_ = {}
        for line in list(
            itertools.takewhile(
                lambda x: "Select jobs" not in x,
                itertools.dropwhile(lambda x: "---" not in x, open(log)),
            )
        ):
            if line.startswith("---") or line.strip() == "":
                continue
            _line = line.split()
            stats_[_line[0]] = JobStats(*_line)

        return stats_

    def __parse_jobs_in_log(self, log):
        """Parse the log file and return a list of job dicts."""
        f = open(log, "r")
        err, msg, jobs, begin = None, "", defaultdict(dict), next(f)
        while line := begin:
            job_dict = dict()
            if line.startswith("[") and not "error" in line:
                timestamp = parser.parse(line.strip("[]\n"))
                job_dict.update({"timestamp": timestamp})

                while (_line := next(f, None)) and not _line.startswith("["):
                    if not _line.strip():
                        continue
                    job_dict.update(parse_rule_grammar(_line.strip(), job_dict))

                if line.startswith("["):
                    begin = _line

                jobs[int(job_dict["jobid"])].update(**job_dict)
            elif any([line.startswith(x) for x in ERROR_LABELS]):
                err, msg = SnakemakeStatus.ERROR, "Snakemake exited with an error"
                break
            elif line.startswith("Unlocking"):
                err, msg = SnakemakeStatus.UNLOCKED, "Snakemake directory was unlocked"
                break
            elif line.startswith("Nothing to be done"):
                err, msg = SnakemakeStatus.SUCCESS, "Nothing to be done"
                break
            else:
                begin = next(f, None)

        f.close()

        return err, msg, jobs

    def _execution_summary(self, n: int = 0):
        # 0-based
        summary_dict = defaultdict(dict)
        err, msg, jobs = self.__parse_jobs_in_log(self.__logs[n])

        if jobs is None:
            return err, msg, None

        for v in jobs.values():
            summary_dict[v["rule"]]["number_of_jobs"] = (
                summary_dict[v["rule"]].get("number_of_jobs", 0) + 1
            )
            if "status" in v:
                summary_dict[v["rule"]][v["status"]] = (
                    summary_dict[v["rule"]].get(v["status"], 0) + 1
                )

        return err, msg, summary_dict

    def history(self, n: int = 10, skip_error: bool = False):
        "View Snakemake execution history with job stats"
        assert n > 0, "-n must be greater than 0"
        n = min(n, len(self.__logs))

        print(
            f"{'timestamp':<20}|{'uuid':<10}|{'num_jobs':<10}|{'queued':<10}|{'failed':<10}|{'finished':<10}"
        )
        print(
            f"{'---------':<20}|{'----':<10}|{'--------':<10}|{'------':<10}|{'------':<10}|{'--------':<10}"
        )
        for i, log in enumerate(self.__logs):
            if i > (n - 1):
                print("... {} more logs".format(len(self.__logs) - n))
                break

            timestamp, uuid, _ = Path(log).stem.split(".")
            _, msg, summary_dict = self._execution_summary(i)  # 0-based

            if not summary_dict:
                if not skip_error:
                    print(
                        f"{parser.parse(timestamp).strftime('%I:%M:%S %p, %b %d'):<20}|\33[31m{uuid:<10}|{msg:<10}\33[0m"
                    )
                continue

            num_jobs, finished, queued, failed, finished = 0, 0, 0, 0, 0
            for _, v in summary_dict.items():
                num_jobs += v.get("number_of_jobs", 0)
                finished += v.get("finished", 0)
                queued += v.get("submitted", 0)
                failed += v.get("failed", 0)

            if finished == num_jobs:
                print(
                    f"{parser.parse(timestamp).strftime('%I:%M:%S %p, %b %d'):<20}|\33[32m{uuid:<10}|{num_jobs:<10}|{queued:<10}|{failed:<10}|{finished:<10}\33[0m"
                )
            else:
                print(
                    f"{parser.parse(timestamp).strftime('%I:%M:%S %p, %b %d'):<20}|\33[33m{uuid:<10}|{num_jobs:<10}|{queued:<10}|{failed:<10}|{finished:<10}\33[0m"
                )

    def status(self, n: int = 1):
        "Get log path and print summary of log file"
        assert n > 0, "-n must be greater than 0"
        n = len(self.__logs) if n > len(self.__logs) else n

        err, msg, summary_dict = self._execution_summary(n - 1)  # 0-based
        stats = self._stats(self.__logs[n - 1])  # 0-based

        print(f"Summary of log file: {self.__logs[n-1]}\n")

        if not summary_dict:
            print(f"{msg}. No jobs to display")
            sys.exit(0)

        # Print rule summaries; remember that one rule may have multiple jobs
        pp_table = []
        total_finished_so_far, total_failed_so_far = 0, 0
        for job, v in stats.items():
            if job == "total":
                continue
            stats_so_far = summary_dict.get(job, defaultdict(int))
            total_finished_so_far += stats_so_far.get("finished", 0)
            total_failed_so_far += stats_so_far.get("failed", 0)
            pp_table.append(
                [f"\33[1m{job.strip(':')}", f"{str(v.count)}\33[0m", f"\33[33m{str(stats_so_far.get('submitted', 0))}\33[0m", f"\33[31m{str(stats_so_far.get('failed', 0))}\33[0m", f"\33[32m{str(stats_so_far.get('finished', 0))}\33[0m"]
            )

        print(tabulate.tabulate(pp_table, headers=["rule", "number_of_jobs", "queued", "failed", "finished"]))

        # Print cute summary after showing job stats
        if total_finished_so_far < int(stats.get("total", JobStats).count):
            phrase, color_begin, color_end = "Only finished", "\33[33m", "\33[0m"
        else:
            phrase, color_begin, color_end = (
                "Successfully finished",
                "\33[32m",
                "\33[0m",
            )

        pct_failed = int(
            (total_failed_so_far / int(stats.get("total", JobStats).count)) * 100
        )
        pct_finished = int(
            (total_finished_so_far / int(stats.get("total", JobStats).count)) * 100
        )
        print(
            f"\n{phrase} {color_begin}{total_finished_so_far} ({pct_finished}%){color_end} and failed \33[31m{total_failed_so_far} ({pct_failed}%)\33[0m of total {stats.get('total', JobStats).count} planned jobs."
        )
        if self._locked and n == 1:
            print("FYI - lock files are present, Snakemake may be running!")

        if err is not None and err != SnakemakeStatus.SUCCESS:
            print(f"{msg}")


    def jobs(self, n: int = 1):
        "Print job summary for n-th most recent execution; detailed status"
        err, msg, jobs = self.__parse_jobs_in_log(self.__logs[n - 1])

        if jobs is None:
            return err, msg, None

        def _color_me(job_status):
            if job_status == "finished":
                return f"\33[32m{job_status}\33[0m"
            elif job_status == "failed":
                return f"\33[31m{job_status}\33[0m"
            else:
                return f"\33[33m{job_status}\33[0m"

        print(
            tabulate.tabulate(
                [
                    [
                        jobid,
                        job["timestamp"],
                        job["rule"],
                        job.get("external_jobid", "-"),
                        _color_me(job['status'])
                    ]
                    for jobid, job in jobs.items()
                ],
                headers=["jobid", "timestamp", "rule", "cluster_jobid", "status"],
            )
        )

    def jobdetails(self, jobid: int, n: int = 1):
        "Print detailed job information for jobid in n-th most recent execution"
        err, msg, jobs = self.__parse_jobs_in_log(self.__logs[n - 1])

        if jobs is None:
            return err, msg, None

        if jobid not in jobs:
            return SnakemakeStatus.ERROR, f"Job {jobid} not found", None

        job = jobs[jobid]
        print(f"Job {jobid} details from logfile: {self.__logs[n-1]}\n----------------")
        for k, v in job.items():
            print(f"{k:<15}: {v}")


def launch():
    if which("snakemake") is None:
        logging.error("snakemake not found in PATH. Please install snakemake.")
        sys.exit(1)

    fire.Fire(SnakeObject, name="snakelog")


if __name__ == "__main__":
    launch()
