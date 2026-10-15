# Distributed MergeSort (`mergesort.py`)

A distributed, parallel sorting program written in Python 3.12 (standard library only).
A **server** splits an unsorted list of integers into chunks and sends them over TCP sockets to
several **clients** (nodes of a Slurm cluster). Each client sorts its chunk using multiple
subprocesses (`multiprocessing.Process`) and sends the result back. The server then merges the
sorted chunks and writes the final, ascending list to an output file.

**Note:** This program was one of my final assignments for a course on parallel and distributed computing during my bachelor's degree.

---

## Contents

- [Requirements](#requirements)
- [Usage](#usage)
  - [Server](#server)
  - [Client](#client)
  - [Running on a Slurm cluster (BiBiGrid / de.NBI Cloud)](#running-on-a-slurm-cluster-bibigrid--denbi-cloud)
  - [Running locally](#running-locally)
- [Input and output format](#input-and-output-format)
- [How it works](#how-it-works)
  - [Overview](#overview)
  - [Socket protocol](#socket-protocol)
  - [Sorting on a client](#sorting-on-a-client)
  - [Pseudo Batcher merge](#pseudo-batcher-merge)
  - [Merging on the server](#merging-on-the-server)
- [LLM notice](#llm-notice)

---

## Requirements

- Python **3.12**
- No third-party packages. Only these standard library modules are used:
  `argparse`, `socket`, `multiprocessing`, `json`, `sys`, `os`, `time`
- For distributed runs: a Slurm cluster (e.g. in my case it was with BiBiGrid on the de.NBI Cloud) in which
  the worker nodes can reach the controller on TCP port **65432**

---

## Usage

The program has two modes (subcommands), `server` and `client`:

```text
python3 mergesort.py server --num-clients <amount-of-clients> --input <input-file> --output <output-file>
python3 mergesort.py client --server-ip <server-ip> --num-procs <amount-of-processes>
```

If you run it without arguments, or with missing or invalid arguments, it prints a short help text
and exits.

### Server

| Parameter       | Required | Description                                                    |
|-----------------|----------|----------------------------------------------------------------|
| `server`        | yes      | Starts the program in server mode                              |
| `--num-clients` | yes      | Number of clients (nodes) the work is split across (integer)   |
| `--input`       | yes      | Path to the file containing the unsorted numbers               |
| `--output`      | yes      | Path of the file the sorted numbers are written to             |

### Client

| Parameter       | Required | Description                                                    |
|-----------------|----------|----------------------------------------------------------------|
| `client`        | yes      | Starts the program in client mode                              |
| `--server-ip`   | yes      | IP address or hostname of the server                           |
| `--num-procs`   | yes      | Number of subprocesses the client uses for sorting (integer)   |

### Running on a Slurm cluster (BiBiGrid / de.NBI Cloud)

1. **Start the server on the Slurm controller** (default: two clients):

   ```bash
   python3 mergesort.py server --num-clients 2 --input unsorted.txt --output sorted.txt
   ```

   The server binds to the IP address its own hostname resolves to and waits for
   `--num-clients` connections.

2. **Start the clients on the worker nodes via Slurm**, from a second shell on the controller:

   ```bash
   srun --nodes=2 python3 mergesort.py client --server-ip <SERVER_NAME_OR_IP> --num-procs <N>
   ```

   - `--nodes` should match `--num-clients` on the server. To use more nodes, raise both values,
     e.g. `--num-clients 4` and `srun --nodes=4 ...`.
   - To use all CPU cores of a node, set `--num-procs` to the number of cores per node
     (check with `nproc` on a worker).
   - `mergesort.py` has to be reachable on every node under the same path, e.g. in a shared
     directory such as `/vol/spool` on BiBiGrid.

3. Once every client has returned its result, the server merges them and writes `sorted.txt`.
   It also prints how long the sort took (the time spent waiting for clients to connect is not
   counted).

> **Calling the script directly (`srun --nodes=2 mergesort.py client ...`)**
> `mergesort.py` starts with the shebang `#!/usr/bin/env python3` and is executable, so it can be
> run without `python3` in front. If the execute permission gets lost (e.g. after copying the file
> to the cluster), restore it with `chmod +x mergesort.py`.
>
> ```bash
> srun --nodes=2 ./mergesort.py client --server-ip <SERVER_NAME_OR_IP> --num-procs <N>
> ```

### Running locally

You can test everything on a single machine by starting the server and the clients in separate
terminals:

```bash
# Terminal 1
python3 mergesort.py server --num-clients 2 --input unsorted.txt --output sorted.txt

# Terminal 2 and 3
python3 mergesort.py client --server-ip "$(hostname)" --num-procs 4
```

---

## Input and output format

- **Input:** a text file with **one integer per line**. Any length works.
  ```text
  42
  -7
  1337
  0
  ```
- **Output:** the same numbers in ascending order, one per line.

---

## How it works

### Overview

```text
                        ┌──────────────────────────────┐
                        │ Server (Slurm controller)    │
                        │ 1. count lines of input      │
                        │ 2. split into num-clients    │
                        │    index ranges              │
                        └──────┬────────────────┬──────┘
                 TCP socket    │                │    TCP socket
                (JSON chunk)   ▼                ▼   (JSON chunk)
              ┌──────────────────────┐    ┌──────────────────────┐
              │ Client 1 (node 1)    │    │ Client 2 (node 2)    │
              │ num-procs processes  │    │ num-procs processes  │
              │ sort + Batcher merge │    │ sort + Batcher merge │
              └──────────┬───────────┘    └───────────┬──────────┘
                         │   sorted chunk (JSON)      │
                         ▼                            ▼
                        ┌──────────────────────────────┐
                        │ Server: Batcher-merge chunks │
                        │ → write output file          │
                        └──────────────────────────────┘
```

1. The server counts the numbers (lines) in the input file and splits the index range into
   `--num-clients` contiguous slices of roughly equal size. The last slice also takes the
   remainder.
2. The server listens on port `65432`. Every client that connects gets the next slice. A separate
   child process on the server handles each connection: it reads its slice from the input file,
   sends it to the client, and waits for the sorted answer. This lets all clients be served and
   work **at the same time**.
3. Each client sorts its slice in parallel with `--num-procs` subprocesses (see below) and sends
   the sorted list back.
4. The server collects the sorted slices from its child processes through
   `multiprocessing.Queue`s, merges them with the pseudo Batcher merge, and writes the result to
   `--output`.

### Socket protocol

Data travels as JSON-encoded lists over TCP. Every message begins with a **10-byte header**
containing the payload size in bytes (ASCII digits, padded with spaces):

```text
┌────────────────────┬──────────────────────────────────┐
│ "123456    "       │ [5, 3, 9, ...]   (123456 bytes)  │
│ 10-byte header     │ JSON payload                     │
└────────────────────┴──────────────────────────────────┘
```

The receiver reads the header first, then keeps calling `recv(4096)` until it has the announced
number of bytes. This makes sure large lists that arrive split across many TCP segments are put
back together correctly.

### Pseudo Batcher merge

`batchermerge(list_a, list_b)` merges two sorted lists, following the idea of Batcher's
odd-even merge:

1. It collects the elements at **even** indices of both lists in one list and the elements at
   **odd** indices in another.
2. It sorts both lists, each in its own child process. Since the result of the recursive odd-even
   merge would simply be a sorted list, `sorted()` takes the place of further recursion.
3. It interleaves the two lists. The first element comes from `even`, the last from the longer
   list. In between, adjacent pairs `even[i]` / `odd[i-1]` are compared and exchanged
   (min first, max second).

Small cases (an empty list, or two lists with one element each) are handled directly.

### Merging on the server

The server merges the sorted client results with `batchermerges_multi_proc` using two processes,
in the same pairwise way as on the clients.

---

## LLM notice

This README was partially written using *Claude (Anthropic, Claude Code)*.
The code itself was written entirely without the help of an LLM.
