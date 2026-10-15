#!/usr/bin/env python3
import argparse
import sys
import socket
import multiprocessing as mp
import json
import time


def sort_in_child(in_queue, out_queue):
    '''
    This function can be used to sort in a child process.

    Its receives a list sorts via a in_queue, sorts it, and sends it back via out_queue.
    '''
    list_to_sort = in_queue.get()
    list_to_sort = sorted(list_to_sort)
    out_queue.put(list_to_sort)
    return


def batchermerge(list_a, list_b, singleProc=False):
    '''
    This function can be used to merge two already sorted lists with a pseudo batchermerge.

    Normally it uses two processes for this process, but it will use a single process
    if singleProc is True.
    '''

    # checking cases where batchermerge is overkill
    if len(list_a) == 0 or len(list_b) == 0:
        return list_a + list_b
    if len(list_a) == 1 and len(list_b) == 1:
        return [min(list_a[0], list_b[0]), max(list_a[0], list_b[0])]
    
    even = list_a[::2] + list_b[::2]
    odd = list_a[1::2] + list_b[1::2]

    # Sorting numbers with even and odd indexes
    # It uses sorted instead of a recursion as the result would just be a sorted list
    # For the reasoning check my report
    if singleProc:
        even = sorted(even)
        odd = sorted(odd)
    else:
        processes = []
        for list_to_sort in [even, odd]:
            in_queue = mp.Queue()
            out_queue = mp.Queue()
            process = mp.Process(target=sort_in_child, args=(in_queue, out_queue))
            processes.append((process, in_queue, out_queue))
            process.start()
            in_queue.put(list_to_sort)

        for i, process in enumerate(processes):
            p, in_q, out_q = process

            if i == 0:
                even = out_q.get()
            else:
                odd = out_q.get()

            p.join()

    merged_list_length = len(even) + len(odd)
    merged_list = [None] * merged_list_length
    merged_list[0] = even[0]
    if len(even) > len(odd):
        merged_list[-1] = even[-1]
    else:
        merged_list[-1] = odd[-1]

    for i in range(1, len(odd)):
        x = even[i]
        y = odd[i - 1]
        merged_list[2*i - 1] = min(x, y)
        merged_list[2*i] = max(x, y)

    # in case that even is 2 elements longer than odd (max edge case), the
    # second last element gets skipped in the loop, so I insert it manually
    if len(even) > len(odd):
        i = len(odd)
        x = even[i]
        y = odd[i - 1]
        merged_list[2*i - 1] = min(x, y)
        merged_list[2*i] = max(x, y)
    
    return merged_list

def batchermerge_in_child(in_queue, out_queue):
    '''
    This function can be used to batchermerge in a child process.

    Its receives two lists and singleProc condition via a in_queue, 
    merges the two lists, and sends the result back via out_queue.
    '''

    list_a, list_b, singleProc = in_queue.get()
    result = batchermerge(list_a, list_b, singleProc)
    out_queue.put(result)
    return


def batchermerges_multi_proc(sorted_list_list, n_procs=2):
    '''
    This function can be used to merge a list of sorted lists.

    It will merge all lists together with a batchermerge and uses 
    generally n_procs processes while doing so.

    '''
    
    # not all procs will be used if its an odd amount
    # For the reasoning check my report
    if n_procs % 2 != 0 and n_procs > 2:
        n_procs -= 1

    if n_procs == 1:
        singleProc = True
    else:
        singleProc = False

    while len(sorted_list_list) > 1:

        processes = []
        merged_lists = []

        pairs = [(i, i+1) for i in range(0, len(sorted_list_list) - 1, 2)]

        if len(sorted_list_list) % 2 != 0:
            merged_lists.append(sorted_list_list[-1])

        # every pair gets 2 processes
        for i, (a, b) in enumerate(pairs):
            processes_in_use = i*2
            if processes_in_use < n_procs:
                in_queue = mp.Queue()
                out_queue = mp.Queue()
                process = mp.Process(target=batchermerge_in_child, args=(in_queue, out_queue))
                processes.append((process, in_queue, out_queue))
                process.start()
                in_queue.put((sorted_list_list[a], sorted_list_list[b], singleProc))
            else:
                merged_lists.append(sorted_list_list[a])
                merged_lists.append(sorted_list_list[b])

        # adding lists that got merged
        for process in processes:
            p, in_q, out_q = process
            merged_lists.append(out_q.get())
            p.join()
        
        # repeat with results unitl its on final list
        sorted_list_list = merged_lists
        
    return sorted_list_list[0]


def batchersort_multi_proc(list_to_sort, n_procs):
    '''
    This function can be used to perform a pseudo batchersort with 
    n_procs processes.

    It starts n_procs processes that will each sort a part of a list_to_sort,
    will merge the resulting sorted lists together with a batchermerge and
    return the result.
    '''

    if n_procs % 2 != 0 and n_procs > 2:
        n_procs -= 1

    processes = []
    result_list = []
    amount_of_numbers = len(list_to_sort)
    batch_size = int(amount_of_numbers/n_procs)

    for proc_id in range(n_procs):
        if proc_id == n_procs - 1:
            start_index = batch_size*proc_id
            end_index = amount_of_numbers
        else:
            start_index = batch_size*proc_id
            end_index = batch_size*(proc_id+1)

        in_queue = mp.Queue()
        out_queue = mp.Queue()
        process = mp.Process(target=sort_in_child, args=(in_queue, out_queue))
        processes.append((process, in_queue, out_queue))
        process.start()
        in_queue.put(list_to_sort[start_index:end_index])

    for process in processes:
        p, in_q, out_q = process
        result_list.append(out_q.get())
        p.join()

    return batchermerges_multi_proc(result_list, n_procs)


def clientCode(server_ip, num_procs, PORT):
    '''
    This function will connect to a server via a socket, receive a list that its supposed to sort,
    sorts the list and sends it back to the server
    '''

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        s.connect((server_ip, PORT))

        # Receiving header of data send by server
        # The header contains the byte size of the data so the client knows when to stop receicing data
        header = s.recv(10)
        if not header:
            print("No header found. Header need to be 10 bytes long and contain byte size of data")
            s.close()
            return

        # Receicing data until the amount of data received is at least equal to the amount its supposed to get
        recv_bytes = []
        data_length_in_bytes = int(header.decode().strip())
        received_bytes = 0
        while received_bytes < data_length_in_bytes:
            data = s.recv(4096)
            if not data:
                break
            recv_bytes.append(data)
            received_bytes += len(data)
        
        # decoding data
        recv_list = json.loads(b"".join(recv_bytes).decode())

        result = batchersort_multi_proc(recv_list, num_procs)

        # encoding results and sending it back to server with a header containting the size
        result_encoded = json.dumps(result).encode()
        result_header = f"{len(result_encoded):<10}".encode()
        s.sendall(result_header + result_encoded)
    finally:
        s.close()


def output_result(output_file, output):
    '''
    This function writes output into output_file
    '''

    try:
        with open(output_file, 'w+') as file:
            file.write(output)
    except:
        print('error while creating final result')


def get_number_amount(input_file):
    '''
    This functions return the amount of numbers/lines a file has
    '''

    try:
        with open(input_file, 'r') as file:
            return len(file.readlines())
    except:
        print(f"can't read file: {input_file}")
        return None


def get_numbers(index_start, index_end, input_file):
    '''
    This function returns the lines from index_start to index_end of the input_file.
    '''

    lines = []
    try:
        with open(input_file, 'r') as file:
            for index, line in enumerate(file):
                if index < index_start:
                    continue

                if index >= index_end:
                    break

                lines.append(int(line))
    except ValueError:
        print("file does contain lines not consisting of numbers")
        return None
    except:
        print(f"can't read file: {input_file}")
        return None
    return lines


def server_handle_client(inSocket, queue, index_start, index_end, input_file):
    '''
    This function can be used in child processes.

    It will send the numbers from index_start to index_end from input_file to a connected socket,
    receive data back from the socket and will send it back to a parent via a queue.
    '''

    numbers_to_check = get_numbers(index_start, index_end, input_file)
    if numbers_to_check is None:
        print(f'Cannot read lines in {input_file}')
        inSocket.close()
        return
    
    # Encoding data to send it via the socket with a header and the data to be processed
    # The header contains the byte size of the data filled up with spaces so the client knows when to stop receicing data
    data_to_send = json.dumps(numbers_to_check).encode()
    header = f"{len(data_to_send):<10}".encode()
    inSocket.sendall(header + data_to_send)
    
    # Waiting until header is received
    header = inSocket.recv(10)
    if not header:
        print("No header found. Header need to be 10 bytes long and contain byte size of data")
        inSocket.close()
        return

    # Receicing data until the amount of data received is at least equal to the amount its supposed to get       
    recv_bytes = []
    data_length_in_bytes = int(header.decode().strip())
    received_bytes = 0
    while received_bytes < data_length_in_bytes:
        data = inSocket.recv(4096)
        if not data:
            break
        recv_bytes.append(data)
        received_bytes += len(data)

    # encoding results and sending it back to parent via queue
    result = json.loads(b"".join(recv_bytes).decode())
    queue.put(result)

    inSocket.close()
    return 


def serverCode(n_clients, input_file, output_file, PORT):
    '''
    This function can be used for a server that is supposed to sort a list of numbers
    contained in input_file.

    It slices the input_file into n_clients pieces, send the slices to clients
    connect via a socket to sort, receive the results from the clients, merge the
    results via batchermerge and write the results into output_file.
    '''

    time_start_of_input_processing = time.perf_counter()

    amount_of_numbers = get_number_amount(input_file)
    if (amount_of_numbers == None):
        return
    
    if n_clients % 2 != 0 and n_clients > 2:
        n_clients -= 1
        print(f'Using an odd amount of workers is inefficient therefore we only accept {n_clients}')
    
    batch_size = int(amount_of_numbers/n_clients)
    task_queue = []
    processes = []
    result_list = []
    HOST = socket.gethostbyname(socket.gethostname()) # IP of my cluster: "192.168.36.147"

    # slice numbers to sort into pieces and save indexes to sort in task_queue
    for i_client in range(n_clients):
        if i_client == n_clients - 1:
            indexes_to_check = (batch_size*i_client, amount_of_numbers)
        else:
            indexes_to_check = (batch_size*i_client, batch_size*(i_client+1))
        task_queue.append(indexes_to_check)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, PORT))
    s.listen(socket.SOMAXCONN)

    no_one_connected = True

    try:
        while len(task_queue) > 0:
            print(f'Waiting for {len(task_queue)} client(s) to connect...')

            if no_one_connected:
                time_until_waiting_for_clients = time.perf_counter()

            inSocket, conInfo = s.accept()
            print(f'Connection from {conInfo}')

            if no_one_connected:
                time_first_connection = time.perf_counter()
                no_one_connected = False

            # indexes for the client to sort
            indexes_to_check = task_queue.pop(0)
            index_start = indexes_to_check[0]
            index_end = indexes_to_check[1]

            # start child process that handles the sending of data to the client
            queue = mp.Queue()
            process = mp.Process(target=server_handle_client , args=(inSocket, queue, index_start, index_end, input_file))
            processes.append((process, queue))
            process.start()
            print("Connected Client received Task.")
            
        amount_processes = len(processes)
        for i, p_and_q in enumerate(processes):
            print(f'Waiting for {amount_processes - i} client(s) to finish merging...')
            p, q = p_and_q
            result_list.append(q.get())
            p.join() 

        print("Merging sorted lists on server...")
        final_list = batchermerges_multi_proc(result_list)

        final_result = "\n".join(str(num) for num in final_list)
        output_result(output_file, final_result)
        print("Done.")
        time_finished = time.perf_counter()
        duration_of_sorting = (time_until_waiting_for_clients - time_start_of_input_processing) + (time_finished - time_first_connection)
        print(f'Time used: {duration_of_sorting:.4f} seconds')
        print(f'Result is saved in {output_file}')
    finally:
        s.close()


########### Code that will run on Server and Client begins here


help_string = "Wrong input!\n\nInput for Server:\nserver --num-clients <amount-of-clients> --input <input-file> --output <output-file>\n\nInput for Client:\nclient --server-ip <server-ip> --num-procs <amount-of-processes>"

# Parser for args that will display help_string on error
class ParserWithHelp(argparse.ArgumentParser):
    def error(self, message):
        print(help_string)
        sys.exit(2)

parser = ParserWithHelp()
subparsers = parser.add_subparsers(dest="mode")

# parse arguments in case of server mode
server = subparsers.add_parser("server")
server.add_argument("--num-clients", type=int, required=True)
server.add_argument("--input", required=True)
server.add_argument("--output", required=True)

# parse arguments in case of client mode
client = subparsers.add_parser("client")
client.add_argument("--server-ip", required=True)
client.add_argument("--num-procs", type=int, required=True)

args = parser.parse_args()
PORT = 65432

# decide for what this python script was called
if args.mode is None:
    print(help_string)
    sys.exit(1)
if args.mode == "server":
    serverCode(args.num_clients, args.input, args.output, PORT)
elif args.mode == "client":
    clientCode(args.server_ip, args.num_procs, PORT)