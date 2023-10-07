#!/usr/bin/env python
import click
import re


pattern_block_begin = re.compile(r" end_log_pos (\d+)")
pattern_block_end = re.compile(r"^# at (\d+)")
pattern_transaction_init = re.compile(r"SET TRANSACTION ISOLATION LEVEL")
pattern_transaction_begin = re.compile(r"^BEGIN")
pattern_transaction_end = re.compile(r"^COMMIT/\*!\*/;")


@click.command()
@click.argument("binlog-file", default="-", type=click.File())
@click.option("-m", "--match-text", default="", help="match text")
def extract_binlog(binlog_file, match_text):
    transaction = []
    is_transaction_matched = False
    is_transaction_end = False
    block = []
    add_block = False
    directive = None
    for line in binlog_file:
        line = line.strip()
        if pattern_block_begin.search(line):
            directive = "add"
            add_block = False
        elif pattern_block_end.match(line):
            directive = "end"

        if match_text in line:
            add_block = True
            is_transaction_matched = True
        elif pattern_transaction_init.search(
            line
        ) or pattern_transaction_begin.match(line):
            add_block = True
            is_transaction_end = False
        elif pattern_transaction_end.match(line):
            add_block = True
            is_transaction_end = True

        match directive:
            case "add":
                block.append(line)
            case "end":  # match..output..clear
                # print("block end", line)
                # print(
                #     f">>> {add_block=}, {is_transaction_matched=}, {is_transaction_end=}"
                # )
                block.append(line)
                if add_block:
                    transaction.extend(block)
                    if is_transaction_end:
                        if is_transaction_matched:
                            # output
                            output_transaction(transaction)
                        # clear & reset
                        transaction = []
                        is_transaction_matched = False
                block = []  # clear block


def match_block(block: list, match_text: str) -> int:
    for line in block:
        if match_text in line:
            return 2
        if (
            pattern_transaction_init.search(line)
            or pattern_transaction_begin.match(line)
            or pattern_transaction_end.match(line)
        ):
            return 1
    return 0


def output_transaction(transaction):
    for line in transaction:
        print(line)
    print("#==============")


# 按间距中的绿色按钮以运行脚本。
if __name__ == "__main__":
    extract_binlog()
