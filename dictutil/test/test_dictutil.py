#!/usr/bin/env python2
# coding: utf-8

import unittest

import dictutil


class TestDictDeepIter(unittest.TestCase):

    def test_depth_iter_default(self):

        cases = (
            ({}, []),
            ({'k1': 'v1'}, [(['k1'], 'v1')]),
            ({'k1': 'v1', 'k2': 'v2'}, [(['k2'], 'v2'), (['k1'], 'v1')]),
            ({'k1': {'k11': 'v11'}, 'k2': 'v2'},
             ([(['k2'], 'v2'), (['k1', 'k11'], 'v11')]),
             )
        )

        for _in, _out in cases:

            idx = 0

            for rst in dictutil.depth_iter(_in):
                self.assertEqual(
                    _out[idx],
                    rst,
                    ('input: {_in}, output: {rst}, expected: {_out}').format(
                        _in=repr(_in),
                        _out=repr(rst),
                        rst=repr(_out[idx])
                    )
                )

                idx = idx + 1

    def test_depth_iter_ks(self):

        idx = 0
        ks = ['mykey']

        _in = {'k1': {'k11': 'v11'}, 'k2': 'v2'}
        _out = [(['mykey', 'k2'], 'v2'), (['mykey', 'k1', 'k11'], 'v11')]
        _mes = 'test argument ks in dictutil.dict_depth_iter()'

        for rst in dictutil.depth_iter(_in, ks=ks):
            self.assertEqual(
                _out[idx],
                rst,
                ('input: {_in}, output: {rst}, expected: {_out},'
                 'message: {_mes}').format(
                     _in=repr(_in),
                     _out=repr(rst),
                     rst=repr(_out[idx]),
                     _mes=_mes
                )
            )

            idx = idx + 1

    def test_depth_iter_maxdepth(self):

        idx = 0

        _in = {'k1': {'k11': {'k111': {'k1111': 'v1111'}}}}
        _out = [(['k1'], {'k11': {'k111': {'k1111': 'v1111'}}}),
                (['k1', 'k11'], {'k111': {'k1111': 'v1111'}}),
                (['k1', 'k11', 'k111'], {'k1111': 'v1111'}),
                (['k1', 'k11', 'k111', 'k1111'], 'v1111')
                ]

        for depth in range(1, 5):
            for rst in dictutil.depth_iter(_in, maxdepth=depth):
                self.assertEqual(
                    _out[idx],
                    rst,
                    'input:: {_in}, output: {rst}, expected: {_out}'.format(
                        _in=repr(_in),
                        rst=repr(rst),
                        _out=repr(_out[idx])
                    )
                )

            idx = idx + 1

    def test_depth_iter_get_mid(self):

        idx = 0

        _in = {'k1': {'k11': {'k111': {'k1111': 'v1111'}}}}
        _out = [(['k1'], {'k11': {'k111': {'k1111': 'v1111'}}}),
                (['k1', 'k11'], {'k111': {'k1111': 'v1111'}}),
                (['k1', 'k11', 'k111'], {'k1111': 'v1111'}),
                (['k1', 'k11', 'k111', 'k1111'], 'v1111')
                ]

        for rst in dictutil.depth_iter(_in, get_mid=True):
            self.assertEqual(
                _out[idx],
                rst)

            idx = idx + 1


class TestDictBreadthIter(unittest.TestCase):

    def test_breadth_iter_default(self):

        cases = (
            ({}, []),
            ({'k1': 'v1'}, [(['k1'], 'v1')]),
            ({'k1': 'v1', 'k2': 'v2'}, [(['k2'], 'v2'), (['k1'], 'v1')]),
            ({'k1': {'k11': 'v11'}, 'k2': 'v2'},
             ([(['k2'], 'v2'),
               (['k1'], {'k11': 'v11'}),
               (['k1', 'k11'], 'v11')
               ])
             )
        )

        for _in, _out in cases:

            idx = 0

            for rst in dictutil.breadth_iter(_in):
                self.assertEqual(
                    _out[idx],
                    rst,
                    ('input: {_in}, output: {rst}, expected: {_out}').format(
                        _in=repr(_in),
                        _out=repr(rst),
                        rst=repr(_out[idx]),
                    )
                )

                idx = idx + 1


class TestAccessor(unittest.TestCase):

    def test_getter_str(self):

        cases = (
            ('',
             'lambda dic, vars={}: dic'
             ),

            ('.',
             'lambda dic, vars={}: dic.get("", {}).get("", vars.get("_default", 0))'
             ),

            ('x',
             'lambda dic, vars={}: dic.get("x", vars.get("_default", 0))'
             ),

            ('x.y',
             'lambda dic, vars={}: dic.get("x", {}).get("y", vars.get("_default", 0))'
             ),

            ('x.y.zz',
             'lambda dic, vars={}: dic.get("x", {}).get("y", {}).get("zz", vars.get("_default", 0))'
             ),

            # dynamic
            ('$',
             'lambda dic, vars={}: dic.get(str(vars.get("", "_")), vars.get("_default", 0))'
             ),

            ('$xx',
             'lambda dic, vars={}: dic.get(str(vars.get("xx", "_")), vars.get("_default", 0))'
             ),

            ('$xx.$yy',
             'lambda dic, vars={}: dic.get(str(vars.get("xx", "_")), {}).get(str(vars.get("yy", "_")), vars.get("_default", 0))'
             ),
        )

        for _in, _out in cases:

            rst = dictutil.make_getter_str(_in)

            self.assertEqual(_out, rst,
                             'input: {_in}, expected: {_out}, actual: {rst}'.format(
                                 _in=repr(_in),
                                 _out=repr(_out),
                                 rst=repr(rst),
                             )
                             )

    def test_getter_str_default(self):

        cases = (

            ('x', None,
             'lambda dic, vars={}: dic.get("x", vars.get("_default", None))'
             ),

            ('x', 1,
             'lambda dic, vars={}: dic.get("x", vars.get("_default", 1))'
             ),

            ('x', True,
             'lambda dic, vars={}: dic.get("x", vars.get("_default", True))'
             ),

            ('x', "abc",
             'lambda dic, vars={}: dic.get("x", vars.get("_default", \'abc\'))'
             ),

        )

        for _in, _default, _out in cases:

            rst = dictutil.make_getter_str(_in, default=_default)

            self.assertEqual(_out, rst,
                             'input: {_in}, {_default}, expected: {_out}, actual: {rst}'.format(
                                 _in=repr(_in),
                                 _default=repr(_default),
                                 _out=repr(_out),
                                 rst=repr(rst),
                             )
                             )

    def test_getter_vars(self):

        cases = (

            ('x', 0,
             {}, {},
             0
             ),

            ('x', 55,
             {}, {},
             55
             ),

            ('x', 55,
             {"x": 1}, {},
             1
             ),

            ('x.y', 55,
             {"x": {"y": 3}}, {},
             3
             ),

            ('x.z', 55,
             {"x": {"y": 3}}, {},
             55
             ),

            ('x.$v', 55,
             {"x": {"y": 3}}, {},
             55
             ),

            ('x.$var_name', 55,
             {"x": {"y": 3}}, {"var_name": "y"},
             3
             ),

            ('x.$var_name.z', 55,
             {"x": {"y": {}}}, {"var_name": "y"},
             55
             ),

            ('x.$var_name.z', 55,
             {"x": {"y": {"z": 4}}}, {"var_name": "y"},
             4
             ),

        )

        for _in, _default, _dic, _vars, _out in cases:

            acc = dictutil.make_getter(_in, default=_default)

            rst = acc(_dic, vars=_vars)

            self.assertEqual(_out, rst,
                             'input: {_in}, {_default}, {_dic}, {_vars} expected: {_out}, actual: {rst}'.format(
                                 _in=repr(_in),
                                 _default=repr(_default),
                                 _dic=repr(_dic),
                                 _vars=repr(_vars),
                                 _out=repr(_out),
                                 rst=repr(rst),
                             )
                             )