from mock import patch, mock_open
from pytest import main
from io import StringIO

from source.payment_engine import CsvTransactionsReader, PaymentsEngine, Reporter


class TestTransactions:

    def test_deposit_withdrawal_dispute_happy_path(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"
                                              "deposit,1,101,20.0\n"
                                              "deposit,2,201,10.0\n"
                                              "withdrawal,1,102,5.0\n"  # Client 1 should have 25.0 available and total
                                              "dispute,2,201,")  # Client 2 should have 10.0 held, 10.0 total

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,25.0,0.0,25.0,false\n' \
                   '2,0.0,10.0,10.0,false\n'

        assert captured == expected

    def test_resolve_pushes_money_back_to_available(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"  # 
                                              "dispute,1,100,\n"
                                              "resolve,1,100,")  # Client 1 should have 10.0 available and total

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,10.0,0.0,10.0,false\n'

        assert captured == expected

    def test_dispute_possible_after_resolve(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"
                                              "dispute,1,100,\n"
                                              "resolve,1,100,\n"
                                              "dispute,1,100,")  # Client 1 should have 10.0 held and total

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,0.0,10.0,10.0,false\n'

        assert captured == expected

    def test_dispute_fails_if_wrong_transaction_id(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"
                                              "dispute,1,101,")  # Wrong transaction id


            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,10.0,0.0,10.0,false\n'

        assert captured == expected

    def test_resolve_fails_if_wrong_transaction_id_or_no_dispute(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"
                                              "resolve,1,100,\n"  # No dispute
                                              "dispute,1,100,\n"  # Client 1 should have 10.0 held and total
                                              "resolve,1,101,\n"  # Wrong transaction id
                                              "deposit,1,101,10.0")  # Client 1 should have 10.0 held and available, 20.0 total

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,10.0,10.0,20.0,false\n'

        assert captured == expected

    def test_chargeback_fails_if_wrong_transaction_id_or_no_dispute(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"
                                              "chargeback,1,100,\n"  # No dispute
                                              "dispute,1,100,\n"  # Client 1 should have 10.0 held and total
                                              "chargeback,1,101,\n"  # Wrong transaction id
                                              "deposit,1,101,10.0")  # Client 1 should have 10.0 held, available and 20.0 total

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,10.0,10.0,20.0,false\n'

        assert captured == expected

    def test_chargeback_withdraws_funds_and_locks_client(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"
                                              "dispute,1,100,\n"
                                              "chargeback,1,100,\n"  # Client 1 should have 0.0 available, held, total and be locked
                                              "deposit,1,101,20.0")  # Shoudn't be accepted due to the locked

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,0.0,0.0,0.0,true\n'

        assert captured == expected

    def test_withdrawal_or_dispute_fails_if_no_funds(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "withdrawal,1,100,15.0\n"  # withdrawal is for Client which even not in database
                                              "deposit,1,100,10.0\n"
                                              "withdrawal,1,101,15.0\n"  # withdrawal is for 15.0 while account is 10.0
                                              "deposit,2,200,10.0\n"
                                              "withdrawal,2,201,5.0\n"
                                              "dispute,2,200,")  # dispute is for 10.0 while account is 5.0

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,10.0,0.0,10.0,false\n' \
                   '2,5.0,0.0,5.0,false\n'

        assert captured == expected

    def test_deposits_and_withdrawals_fail_if_duplicated_transaction_id(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"
                                              "withdrawal,1,100,5.0\n"  # duplicated transaction id with deposit
                                              "deposit,2,100,10.0")  # duplicated Client 2 transaction id with Client 1 deposit

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,10.0,0.0,10.0,false\n' \
                   '2,0.0,0.0,0.0,false\n'

        assert captured == expected

    def test_deposits_and_withdrawals_fail_if_wrong_amount(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,100,10.0\n"
                                              "withdrawal,1,101,-5.0\n"  # negative value
                                              "deposit,2,102,-10.0\n"   # negative value
                                              "deposit,3,103,0.0\n"
                                              "withdrawal,1,101,0.0")  # zero value

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,10.0,0.0,10.0,false\n' \

        assert captured == expected

    def test_correct_output_formatting(self, capsys):

        with patch('builtins.open', new_callable=mock_open) as open_mock:
            open_mock.return_value = StringIO("type,client,tx,amount\n"
                                              "deposit,1,101,10.1\n"
                                              "deposit,2,102,10.12\n"
                                              "deposit,3,103,10.123\n"
                                              "deposit,4,104,10.1234\n")

            bank = PaymentsEngine(CsvTransactionsReader('dummy'), Reporter())
            bank.run()
            captured, _ = capsys.readouterr()

        expected = 'client,available,held,total,locked\n' \
                   '1,10.1,0.0,10.1,false\n' \
                   '2,10.12,0.0,10.12,false\n' \
                   '3,10.123,0.0,10.123,false\n' \
                   '4,10.1234,0.0,10.1234,false\n'

        assert captured == expected


if __name__ == '__main__':
    main()