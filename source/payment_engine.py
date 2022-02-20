""" Simple payment engine.

Writing to stdout
Example::
python payment_engine.py <NAME>.csv

Writing to file
Example::
python payment_engine.py <NAME>.csv > <NAME>.csv

"""
import logging
import sys

import numpy
import pandas


logging.basicConfig(format='%(levelname)s:%(message)s')
logging.getLogger().setLevel(logging.DEBUG)


class Account:

    """Gather client's balance."""

    def __init__(self):
        self.available = 0.0
        self.held = 0.0
        self.locked = False

    @property
    def total(self):
        """Get total funds."""
        return self.available + self.held

    def get_balance(self):
        """Get account balance."""
        locked = 'false' if self.locked is False else 'true'
        return f'{self.available},{self.held},{self.total},{locked}'


class Client:

    """Gather Client's data."""

    def __init__(self, client_id):
        self.id = client_id
        self.account = Account()

    def get_balance(self):
        """Get client's balance."""
        return f'{self.id},' + self.account.get_balance()


class ClientsBalancesReporter:

    """Report all clients' balances."""

    def __init__(self, clients):
        self._clients = clients

    @staticmethod
    def get_header():
        """Get fields names."""
        return 'client,available,held,total,locked'

    def get_balances(self):
        """Get all clients' balances."""
        for client in self._clients.values():
            yield client.get_balance()


class CmdParser:

    """Parse command line execution arguments."""

    def __init__(self):
        self._data = sys.argv[1:]
        self._input_file = ''
        self._update()

    def _update(self):
        if self._data:
            self._input_file = self._data[0]

    @property
    def input_file(self):
        """Get input file name."""
        return self._input_file


class CsvTransactionsReader:

    """Read transactions from csv."""

    DATA_TYPES = {'client': numpy.uint16,
                  'tx': numpy.uint32,
                  'amount': float}

    def __init__(self, path):
        self._path = path

    def _get_record_from_file(self):
        reader = pandas.read_csv(self._path, iterator=True, chunksize=1, dtype=self.DATA_TYPES)
        for row in reader:
            yield [row.type.values[0], row.client.values[0], row.tx.values[0], row.amount.values[0]]
        logging.info('All transactions processed')

    def get(self):
        """Get chunk of data."""
        return self._get_record_from_file()


class TransactionType:

    """Transaction Types."""

    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    DISPUTE = "dispute"
    RESOLVE = "resolve"
    CHARGEBACK = "chargeback"
    ALLTYPES = {DEPOSIT, WITHDRAWAL, DISPUTE, RESOLVE, CHARGEBACK}


class Transaction:

    """Client's transaction."""

    def __init__(self, transaction_type, client_id, transaction_id, amount):
        self.type = transaction_type
        self.client_id = client_id
        self.id = transaction_id
        self.amount = amount


class TransactionsCreator:

    """Create valid transactions."""

    def __init__(self, input_reader, validator):
        self._input_reader = input_reader
        self._validator = validator

    def get(self):
        """Get valid transaction."""
        for record in self._input_reader.get():
            transaction = Transaction(*record)
            if self._validator.is_valid(transaction):
                yield transaction


class TransactionValidator:

    """Validate transaction data correctness."""

    def is_valid(self, transaction):
        """Check transaction correctness."""
        return self._is_valid_type(transaction.type) and self._is_greater_than_zero(transaction)

    @staticmethod
    def _is_valid_type(transaction_type):
        return transaction_type in TransactionType.ALLTYPES

    @staticmethod
    def _is_greater_than_zero(transaction):
        if transaction.type in [TransactionType.DEPOSIT, TransactionType.WITHDRAWAL]:
            return transaction.amount > 0
        return True


class Reporter:

    """Report data provided."""

    @staticmethod
    def write(data):
        """Write data provided."""
        print(data, flush=True)


class PaymentsEngine:

    """Handle payments."""

    def __init__(self, input_data, output):
        self._transactions_parser = TransactionsCreator(input_data, TransactionValidator())
        self._deposits_withdrawals = {}
        self._disputed_deposits_ids = []
        self._clients = {}
        self._output = output

    def run(self):
        """Handle transactions."""
        for transaction in self._transactions_parser.get():
            self._handle_transaction(transaction)

        balances_reporter = ClientsBalancesReporter(self._clients)

        self._output.write(balances_reporter.get_header())

        for balance in balances_reporter.get_balances():
            self._output.write(balance)

    def _handle_transaction(self, transaction):

        client = self._clients.setdefault(transaction.client_id, Client(transaction.client_id))

        if not client.account.locked:

            if transaction.type == TransactionType.DEPOSIT:
                if self._is_transaction_unique(transaction.id):
                    self._deposit(transaction, client)
                else:
                    logging.error('Duplicated transaction id')

            elif transaction.type == TransactionType.WITHDRAWAL:
                if self._is_transaction_unique(transaction.id):
                    self._withdrawal(transaction, client)
                else:
                    logging.error('Duplicated transaction id')

            elif transaction.type == TransactionType.DISPUTE:
                self._dispute(transaction, client)

            elif transaction.type == TransactionType.RESOLVE:
                self._resolve(transaction, client)

            elif transaction.type == TransactionType.CHARGEBACK:
                self._chargeback(transaction, client)

        else:
            logging.error('Client account is locked, could not perform operation')

    def _deposit(self, transaction, client):
        self._deposits_withdrawals[transaction.id] = transaction
        client.account.available += transaction.amount

    def _withdrawal(self, transaction, client):
        if self._are_enough_funds(transaction.amount, client):
            self._deposits_withdrawals[transaction.id] = transaction
            client.account.available -= transaction.amount
        else:
            logging.error('Not enough funds available')

    def _dispute(self, transaction, client):
        if self._is_transaction_disputable(transaction) and self._is_client_correct(transaction):
            amount = self._deposits_withdrawals[transaction.id].amount
            if self._are_enough_funds(amount, client):
                self._disputed_deposits_ids.append(transaction.id)
                client.account.available -= amount
                client.account.held += amount
        else:
            logging.error('Dispute not possible')

    def _resolve(self, transaction, client):
        if self._is_under_dispute(transaction) and self._is_client_correct(transaction):
            self._disputed_deposits_ids.remove(transaction.id)
            amount = self._deposits_withdrawals[transaction.id].amount
            client.account.held -= amount
            client.account.available += amount
        else:
            logging.error('Resolve not possible')

    def _chargeback(self, transaction, client):
        if self._is_under_dispute(transaction) and self._is_client_correct(transaction):
            self._disputed_deposits_ids.remove(transaction.id)
            client.account.held -= self._deposits_withdrawals[transaction.id].amount
            client.account.locked = True
        else:
            logging.error('Chargeback not possible')

    def _is_transaction_unique(self, transaction_id):
        return transaction_id not in self._deposits_withdrawals

    def _is_transaction_disputable(self, transaction):
        return transaction.id in self._deposits_withdrawals and not self._is_under_dispute(transaction)

    def _is_client_correct(self, transaction):
        return transaction.client_id == self._deposits_withdrawals[transaction.id].client_id

    def _is_under_dispute(self, transaction):
        return transaction.id in self._disputed_deposits_ids

    @staticmethod
    def _are_enough_funds(amount, client):
        return client.account.available >= amount


def main():
    """Run payment engine."""

    parser = CmdParser()
    bank = PaymentsEngine(CsvTransactionsReader(parser.input_file), Reporter())
    bank.run()


if __name__ == '__main__':
    main()
