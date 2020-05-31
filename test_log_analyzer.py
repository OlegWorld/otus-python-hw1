import unittest
import log_analyzer


class TestLogAnalyzer(unittest.TestCase):
    def test_read_config(self):
        self.assertTrue(log_analyzer.read_config('config.json'))
        self.assertFalse(log_analyzer.read_config('some_file'))

    def test_read_argv(self):
        self.assertFalse(log_analyzer.read_argv(['filename.py', 'some_str']))
        self.assertFalse(log_analyzer.read_argv(['filename.py']))
        self.assertTrue(log_analyzer.read_argv(['filename.py', '--config']))
        self.assertTrue(log_analyzer.read_argv(['filename.py', '--config', 'config.json']))

    def test_statistics(self):
        statistics = log_analyzer.Statistics()
        statistics.update(('/some/url', 0.1))
        statistics.update(('/other/url', 0.2))
        statistics.update(('/some/url', 0.4))

        report = statistics.get_report(report_size=10)

        self.assertEqual(len(report), 2)
        self.assertEqual(report[0]['url'], '/some/url')
        self.assertEqual(report[1]['url'], '/other/url')
        self.assertAlmostEqual(report[0]['time_sum'], 0.5)
        self.assertAlmostEqual(report[1]['time_sum'], 0.2)


if __name__ == '__main__':
    unittest.main()
