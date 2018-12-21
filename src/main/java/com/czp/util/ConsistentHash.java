package com.czp.util;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * 一致性hash
 *
 * @param <T> 节点类型
 */
public class ConsistentHash<T> {
    /**
     * Hash计算对象，用于自定义hash算法
     */
    private Function<String, Integer> hashMethod;
    /**
     * 复制的节点个数
     */
    private final int numberOfReplicas;
    /**
     * 一致性Hash环 环上对应的是虚拟节点
     */
    private final SortedMap<Integer, T> circle = new TreeMap<>();
    /**
     * node转化为string的方法，默认会使用node.toString()
     */
    private Function<T, String> node2Str;

    public ConsistentHash(int numberOfReplicas, Collection<T> nodes) {
        this(numberOfReplicas, nodes, T::toString, (String input) -> {
            HashCode hashCode = Hashing.goodFastHash(32).hashString(input, Charset.forName("UTF-8"));
            return hashCode.asInt();
        });
    }


    public ConsistentHash(int numberOfReplicas, Collection<T> nodes, Function<T, String> node2Str) {
        this(numberOfReplicas, nodes, node2Str, (String input) -> {
            HashCode hashCode = Hashing.goodFastHash(32).hashString(input, Charset.forName("UTF-8"));
            return hashCode.asInt();
        });
    }

    /**
     * @param numberOfReplicas 复制的虚拟节点数量
     * @param nodes            节点列表
     * @param node2Str         节点转String的方法
     * @param hashMethod       哈希函数
     */
    public ConsistentHash(int numberOfReplicas, Collection<T> nodes, Function<T, String> node2Str,
                          Function<String, Integer> hashMethod) {
        this.numberOfReplicas = numberOfReplicas;
        this.node2Str = node2Str;
        this.hashMethod = hashMethod;
        //初始化节点
        for (T node : nodes) {
            add(node);
        }
    }

    /**
     * 增加节点<br>
     * 每增加一个节点，就会在闭环上增加给定复制节点数<br>
     * 例如复制节点数是2，则每调用此方法一次，增加两个虚拟节点，这两个节点指向同一Node
     * 由于hash算法会调用node的toString方法，故按照toString去重
     *
     * @param node 节点对象
     */
    public void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.put(hashMethod.apply(node2Str.apply(node) + "#" + i), node);
        }
    }

    /**
     * 移除节点的同时移除相应的虚拟节点
     *
     * @param node 节点对象
     */
    public void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hashMethod.apply(node2Str.apply(node) + "#" + i));
        }
    }

    /**
     * 获得一个最近的顺时针节点
     *
     * @param key 为给定键取Hash，取得顺时针方向上最近的一个虚拟节点对应的实际节点
     * @return 节点对象
     */
    public T get(Object key) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = hashMethod.apply(key.toString());
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, T> tailMap = circle.tailMap(hash); //返回此映射的部分视图，其键大于等于 hash
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        //正好命中
        return circle.get(hash);
    }

}
